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

type PostingType uint8

const (
	PostingTypeText PostingType = iota
	PostingTypeTag
	PostingTypeEnd
)

type PostingTag uint8

const (
	PostingTagEmtpy PostingTag = iota
	PostingTagTitle
	PostingTagH1
	PostingTagH2
	PostingTagH3
	PostingTagB
	PostingTagI
)

type Posting struct {
	Type  PostingType
	DocID uint64
	Tag   string
	Pos   uint16
}

func (p Posting) ID() string {
	return fmt.Sprintf("%d-%d-%s-%d", p.Type, p.DocID, p.Tag, p.Pos)
}

func (p Posting) Size() int {
	return 8*4 + len(p.Tag)
}

func ConvertFromTagString(s string) PostingTag {
	switch s {
	case "title":
		return PostingTagTitle
	case "h1":
		return PostingTagH1
	case "h2":
		return PostingTagH2
	case "h3":
		return PostingTagH3
	case "b":
		return PostingTagB
	case "i":
		return PostingTagI
	default:
		return PostingTagEmtpy
	}
}

func ConvertToTagString(tag PostingTag) string {
	switch tag {
	case PostingTagTitle:
		return "title"
	case PostingTagH1:
		return "h1"
	case PostingTagH2:
		return "h2"
	case PostingTagH3:
		return "h3"
	case PostingTagB:
		return "b"
	case PostingTagI:
		return "i"
	default:
		return ""
	}
}

func ParsePostings(doc parser.Doc, index PartialIndex, stats *IndexStats) {
	var pos uint16

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

	for _, token := range doc.OriginalTokens {
		posting := Posting{
			Type:  PostingTypeText,
			DocID: doc.ID,
			Pos:   0, // not used
		}
		index[token] = append(index[token], posting)
		stats.AddTerm(doc.ID, token)
	}

	pos = 0
	for _, token := range doc.TwoGrams {
		posting := Posting{
			Type:  PostingTypeText,
			DocID: doc.ID,
			Pos:   pos,
		}
		index[token] = append(index[token], posting)
		stats.AddTerm(doc.ID, token)
		pos++
	}

	pos = 0
	for _, token := range doc.ThreeGrams {
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
		pos = 0
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

	pType, docIDLen, err := ReadPostingHeader(br)
	if err != nil {
		return posting, err
	}
	posting.Type = pType
	if posting.Type == PostingTypeEnd {
		return posting, ErrPostingTypeEnd
	}

	pDocID, err := readDocID(br, docIDLen)
	if err != nil {
		return posting, err
	}
	posting.DocID = pDocID

	if posting.Type == PostingTypeTag {
		tag, err := br.ReadUInt8()
		if err != nil {
			return posting, err
		}
		pTag := ConvertToTagString(PostingTag(tag))
		posting.Tag = pTag
	}

	pPos, err := br.ReadUInt16()
	if err != nil {
		return posting, err
	}
	posting.Pos = pPos

	return posting, nil
}

// PostingType, DocIDLen
func ReadPostingHeader(br *binary.ByteReader) (PostingType, uint8, error) {
	header, err := br.ReadUInt8()
	if err != nil {
		return 0, 0, err
	}
	pType, docIDLen := decodeHeader(header)
	return pType, docIDLen, nil
}

func decodeHeader(header uint8) (PostingType, uint8) {
	pType := (header >> 4) & 15
	docIDLen := header & 15
	return PostingType(pType), docIDLen
}

func readDocID(br *binary.ByteReader, docIDLen uint8) (uint64, error) {
	switch docIDLen {
	case 0:
		docID, err := br.ReadUInt8()
		return uint64(docID), err
	case 1:
		docID, err := br.ReadUInt16()
		return uint64(docID), err
	case 2:
		docID, err := br.ReadUInt32()
		return uint64(docID), err
	default:
		return br.ReadUInt64()
	}
}

func WritePosting(bw *binary.ByteWriter, posting Posting) error {
	docIDLen, err := WritePostingHeader(bw, posting.Type, posting.DocID)
	if err != nil {
		return err
	}

	if err := writeDocID(bw, docIDLen, posting.DocID); err != nil {
		return err
	}

	// if err := bw.WriteUInt8(uint8(posting.Type)); err != nil {
	// 	return err
	// }
	// if err := bw.WriteUInt64(posting.DocID); err != nil {
	// 	return err
	// }

	if posting.Type == PostingTypeTag {
		tag := ConvertFromTagString(posting.Tag)
		if tag != PostingTagEmtpy {
			if err := bw.WriteUInt8(uint8(tag)); err != nil {
				return err
			}
		}
	}
	if err := bw.WriteUInt16(posting.Pos); err != nil {
		return err
	}
	return nil
}

func WritePostingHeader(bw *binary.ByteWriter, pType PostingType, docID uint64) (uint8, error) {
	var docIDLen uint8
	if docID <= 255 {
		docIDLen = 0
	} else if docID <= 65535 {
		docIDLen = 1
	} else if docID <= 4_294_967_295 {
		docIDLen = 2
	} else {
		docIDLen = 3
	}
	// docIDLen = 3
	header := encodeHeader(pType, docIDLen)
	return docIDLen, bw.WriteUInt8(header)
}

func encodeHeader(pType PostingType, docIDLen uint8) uint8 {
	return uint8(pType)<<4 + docIDLen
}

func writeDocID(bw *binary.ByteWriter, docIDLen uint8, docID uint64) error {
	switch docIDLen {
	case 0:
		return bw.WriteUInt8(uint8(docID))
	case 1:
		return bw.WriteUInt16(uint16(docID))
	case 2:
		return bw.WriteUInt32(uint32(docID))
	default:
		return bw.WriteUInt64(docID)
	}
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
