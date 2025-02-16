package indexer

import (
	"io"
	"iter"
	"log"
	"os"
	"petersearch/pkg/parser"
)

type PostingType int

const (
	PostingTypeText PostingType = iota
	PostingTypeTag
)

type Posting struct {
	Type  PostingType
	DocID int
	Tag   string
	Pos   int
}

func ParsePostings(doc parser.Doc, index PartialIndex, docStats *DocStats) {
	pos := 0
	for _, token := range doc.Tokens {
		posting := Posting{
			Type:  PostingTypeText,
			DocID: doc.ID,
			Pos:   pos,
		}
		index[token] = append(index[token], posting)
		docStats.AddTerm(doc.ID, token)
		pos++
	}

	for tag, tokens := range doc.TagMap {
		for _, token := range tokens {
			posting := Posting{
				Type:  PostingTypeTag,
				DocID: doc.ID,
				Tag:   tag,
			}
			index[token] = append(index[token], posting)
			docStats.AddTerm(doc.ID, token)
			pos++
		}
	}
}

func ReadPosting(br *ByteReader) (Posting, error) {
	var posting Posting

	pType, err := br.ReadInt()
	if err != nil {
		return posting, err
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

func WritePosting(bw *ByteWriter, posting Posting) error {
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

func PostingsIterator(br *ByteReader, length int) iter.Seq2[int, Posting] {
	count := 0
	return func(yield func(int, Posting) bool) {
		for range length {
			posting, err := ReadPosting(br)
			if err != nil {
				log.Fatalf("failed to read posting: %v", err)
			}

			if !yield(count, posting) {
				return
			}
		}
	}
}

type InvertedListIter struct {
	Term   string
	Length int
	// PostingIter iter.Seq2[int, Posting]
	Next func() (int, Posting, bool)
	Stop func()
}

func ReadInvertedList(br *ByteReader) (InvertedListIter, error) {
	var list InvertedListIter
	term, err := br.ReadString()
	if err != nil {
		return list, err
	}
	list.Term = term

	length, err := br.ReadInt()
	if err != nil {
		return list, err
	}
	list.Length = length

	postingIter := PostingsIterator(br, length)
	next, stop := iter.Pull2(postingIter)
	list.Next = next
	list.Stop = stop

	return list, nil
}

func InvertedListIterator(fileName string) iter.Seq2[int, InvertedListIter] {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	count := 0
	br := NewByteReader(NewBufferedReadCloser(f))
	return func(yield func(int, InvertedListIter) bool) {
		for {
			list, err := ReadInvertedList(br)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(f)
			}
			if !yield(count, list) {
				return
			}
			count++
		}
	}
}
