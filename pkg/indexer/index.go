package indexer

import (
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"slices"
	"sort"
)

type DocStats struct {
	DocCount       int
	AllTermFreqs   map[string]int
	DocIDToURL     map[int]string
	URLToDocID     map[string]int
	DocIDTermFreqs map[int]map[string]int
}

func NewDocStats() *DocStats {
	return &DocStats{
		AllTermFreqs:   map[string]int{},
		DocIDToURL:     map[int]string{},
		URLToDocID:     map[string]int{},
		DocIDTermFreqs: map[int]map[string]int{},
	}
}

func (stats *DocStats) AddDoc(doc parser.Doc) {
	stats.DocCount++
	stats.DocIDToURL[doc.ID] = doc.URL
	stats.URLToDocID[doc.URL] = doc.ID
}

func (stats *DocStats) AddTerm(docID int, term string) {
	stats.AllTermFreqs[term]++
	if _, ok := stats.DocIDTermFreqs[docID]; !ok {
		stats.DocIDTermFreqs[docID] = map[string]int{}
	}
	stats.DocIDTermFreqs[docID][term]++
}

type PartialIndex map[string][]Posting

func (p PartialIndex) Print(prefix string) {
	terms := []string{}
	for term := range p {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		fmt.Println(prefix, term, p[term])
	}
}

func (p PartialIndex) SortedList() []InvertedList {
	terms := []string{}
	for term := range p {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	list := []InvertedList{}
	for _, term := range terms {
		slices.SortFunc(p[term], SortPostingsComparator())
		list = append(list, InvertedList{
			Term:     term,
			Postings: p[term],
		})
	}

	return list
}

func (p PartialIndex) SortedIter() PartialIndexIter {
	var outIter PartialIndexIter

	iterFunc := func(yield func(int, InvertedListIter) bool) {
		terms := []string{}
		for term := range p {
			terms = append(terms, term)
		}
		sort.Strings(terms)

		for i, term := range terms {
			slices.SortFunc(p[term], SortPostingsComparator())
			listIter := InvertedListIterator(term, p[term])
			if !yield(i, listIter) {
				return
			}
		}
	}

	next, stop := iter.Pull2(iterFunc)
	outIter.Next = next
	outIter.Stop = stop

	return outIter
}

type PartialIndexIter struct {
	Next func() (int, InvertedListIter, bool)
	Stop func()
}

func FilePartialIndexIterator(file string) PartialIndexIter {
	var indexIter PartialIndexIter
	listIter := FileInvertedListIterator(file)
	next, stop := iter.Pull2(listIter)
	indexIter.Next = next
	indexIter.Stop = stop
	return indexIter
}

func ReadFilePartialIndex(file string) (PartialIndex, error) {
	f, err := os.Open(file)
	if err != nil {
		return PartialIndex{}, err
	}

	br := NewByteReader(NewBufferedReadCloser(f))
	return ReadPartialIndex(br)
}

func ReadPartialIndex(br *ByteReader) (PartialIndex, error) {
	index := PartialIndex{}

	for {
		term, err := br.ReadString()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for {
			posting, err := ReadPosting(br)
			if err == ErrPostingTypeEnd {
				break
			}
			if err != nil {
				return nil, err
			}
			index[term] = append(index[term], posting)
		}
	}

	return index, nil
}

func WritePartialIndex(bw *ByteWriter, index PartialIndex) error {
	invertedLists := index.SortedList()
	for _, list := range invertedLists {
		term, postings := list.Term, list.Postings
		if err := bw.WriteString(term); err != nil {
			return err
		}
		for _, posting := range postings {
			WritePosting(bw, posting)
		}
		if err := bw.WriteInt(int(PostingTypeEnd)); err != nil {
			return err
		}
	}
	return nil
}

func BuildPartialIndex(batch int, producer stream.Producer, indexConsumer, statsConsumer stream.Consumer) {
	docStats := NewDocStats()
	defer func() {
		statsConsumer.Consume(docStats)
	}()

	count := 0
	index := PartialIndex{}
	for {
		v, ok := producer.Produce()
		if !ok {
			if count != 0 {
				indexConsumer.Consume(index)
			}
			break
		}

		doc := v.(parser.Doc)
		docStats.AddDoc(doc)
		ParsePostings(doc, index, docStats)

		count++
		if count == batch {
			indexConsumer.Consume(index)
			count = 0
			index = PartialIndex{}
		}
	}
}

func SavePartialIndex(dir string, producer stream.Producer, consumer stream.Consumer) {
	for {
		v, ok := producer.Produce()
		if !ok {
			break
		}

		index := v.(PartialIndex)
		file, err := os.CreateTemp(dir, "partial-index")
		if err != nil {
			log.Fatal(err)
		}

		bw := NewByteWriter(file)
		if err = WritePartialIndex(bw, index); err != nil {
			log.Fatal(err)
		}

		consumer.Consume(file.Name())
	}
}
