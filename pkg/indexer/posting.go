package indexer

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/binary"
)

var ErrPostingTypeEnd = errors.New("posting list ends")

type PostingType int

const (
	PostingTypeText PostingType = iota
	PostingTypeTag
	PostingTypeEnd
)

type Posting struct {
	Type  PostingType
	DocID int
	Tag   string
	Pos   int
}

func (p Posting) ID() string {
	return fmt.Sprintf("%d-%d-%s-%d", p.Type, p.DocID, p.Tag, p.Pos)
}

func (p Posting) Size() int {
	return 8*4 + len(p.Tag)
}

func ParsePostings(doc parser.Doc, index PartialIndex, stats *IndexStats) {
	pos := 0
	for _, token := range doc.Tokens {
		posting := Posting{
			Type:  PostingTypeText,
			DocID: doc.ID,
			Pos:   pos,
		}
		index[token] = append(index[token], posting)
		stats.AddTerm(doc.ID, token)
		pos++
	}

	for tag, tokens := range doc.TagMap {
		pos := 0
		for _, token := range tokens {
			posting := Posting{
				Type:  PostingTypeTag,
				DocID: doc.ID,
				Tag:   tag,
				Pos:   pos,
			}
			index[token] = append(index[token], posting)
			stats.AddTerm(doc.ID, token)
			pos++
		}
	}
}

func ReadPosting(br *binary.ByteReader) (Posting, error) {
	var posting Posting

	pType, err := br.ReadInt()
	if err != nil {
		return posting, err
	}
	if pType == int(PostingTypeEnd) {
		return posting, ErrPostingTypeEnd
	}
	posting.Type = PostingType(pType)

	pDocID, err := br.ReadInt()
	if err != nil {
		return posting, err
	}
	posting.DocID = pDocID

	if posting.Type == PostingTypeTag {
		pTag, err := br.ReadString()
		if err != nil {
			return posting, err
		}
		posting.Tag = pTag
	}

	pPos, err := br.ReadInt()
	if err != nil {
		return posting, err
	}
	posting.Pos = pPos

	return posting, nil
}

func WritePosting(bw *binary.ByteWriter, posting Posting) error {
	if err := bw.WriteInt(int(posting.Type)); err != nil {
		return err
	}
	if err := bw.WriteInt(posting.DocID); err != nil {
		return err
	}
	if posting.Type == PostingTypeTag {
		if err := bw.WriteString(posting.Tag); err != nil {
			return err
		}
	}
	if err := bw.WriteInt(posting.Pos); err != nil {
		return err
	}
	return nil
}

func PostingsIterator(br *binary.ByteReader) iter.Seq2[int, Posting] {
	return func(yield func(int, Posting) bool) {
		count := 0
		for {
			posting, err := ReadPosting(br)
			if err == ErrPostingTypeEnd {
				break
			}
			if err != nil {
				log.Fatalf("failed to read posting: %v", err)
			}

			if !yield(count, posting) {
				return
			}
		}
	}
}

func SortPostingsComparator() func(Posting, Posting) int {
	type Comparator func(Posting, Posting) int

	compareType := func(p1, p2 Posting) int {
		return cmp.Compare(p1.Type, p2.Type)
	}
	compareDocID := func(p1, p2 Posting) int {
		return cmp.Compare(p1.DocID, p2.DocID)
	}
	compareTag := func(p1, p2 Posting) int {
		return cmp.Compare(p1.Tag, p2.Tag)
	}
	comparePos := func(p1, p2 Posting) int {
		return cmp.Compare(p1.Pos, p2.Pos)
	}

	return func(p1, p2 Posting) int {
		comparators := []Comparator{compareType, compareDocID, compareTag, comparePos}
		for _, comp := range comparators {
			if r := comp(p1, p2); r != 0 {
				return r
			}
		}
		return 0
	}
}

type InvertedList struct {
	Term     string
	Postings []Posting
}

type InvertedListIter struct {
	Term string
	// PostingIter iter.Seq2[int, Posting]
	Next func() (int, Posting, bool)
	Stop func()
}

func InMemoryInvertedListIterator(list InvertedList) InvertedListIter {
	var listIter InvertedListIter

	iterFunc := func(yield func(int, Posting) bool) {
		for i, posting := range list.Postings {
			if !yield(i, posting) {
				return
			}
		}
	}

	listIter.Next, listIter.Stop = iter.Pull2(iterFunc)

	return listIter
}

func CollectInvertedList(listIter InvertedListIter) []Posting {
	defer listIter.Stop()
	var postings []Posting

	for {
		_, posting, ok := listIter.Next()
		if !ok {
			break
		}
		postings = append(postings, posting)
	}

	return postings
}

func ReadInvertedList(br *binary.ByteReader) (InvertedListIter, error) {
	var list InvertedListIter
	term, err := br.ReadString()
	if err != nil {
		return list, err
	}
	list.Term = term

	postingIter := PostingsIterator(br)
	next, stop := iter.Pull2(postingIter)
	list.Next = next
	list.Stop = stop

	return list, nil
}

func InvertedListIterator(term string, postings []Posting) InvertedListIter {
	var listIter InvertedListIter
	listIter.Term = term

	iterFunc := func(yield func(int, Posting) bool) {
		count := 0
		for _, posting := range postings {
			if !yield(count, posting) {
				return
			}
			count++
		}
	}

	next, stop := iter.Pull2(iterFunc)
	listIter.Next = next
	listIter.Stop = stop

	return listIter
}

func FileInvertedListIterator(fileName string) iter.Seq2[int, InvertedListIter] {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal("failed to open file:", err)
	}

	count := 0
	br := binary.NewBufferedByteReader(f)
	return func(yield func(int, InvertedListIter) bool) {
		defer f.Close()
		for {
			list, err := ReadInvertedList(br)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("failed to read inverted list:", err)
			}
			if !yield(count, list) {
				return
			}
			count++
		}
	}
}
