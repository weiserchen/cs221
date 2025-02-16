package indexer

import (
	"io"
	"iter"
	"log"
	"os"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"sort"

	pq "github.com/emirpasic/gods/v2/queues/priorityqueue"
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

func ReadPartialIndex(br *ByteReader) (PartialIndex, error) {
	index := PartialIndex{}

	for {
		// fmt.Println("start ok")
		term, err := br.ReadString()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// fmt.Println("term ok")

		length, err := br.ReadInt()
		if err != nil {
			return nil, err
		}

		// fmt.Println("length ok")
		for range length {
			posting, err := ReadPosting(br)
			if err != nil {
				return nil, err
			}
			index[term] = append(index[term], posting)
		}
	}

	return index, nil
}

func WritePartialIndex(index PartialIndex, bw *ByteWriter) error {
	for term, postings := range index {
		if err := bw.WriteString(term); err != nil {
			return err
		}
		if err := bw.WriteInt(len(postings)); err != nil {
			return err
		}
		for _, posting := range postings {
			WritePosting(bw, posting)
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
		err = WritePartialIndex(index, bw)
		if err != nil {
			log.Fatal(err)
		}

		consumer.Consume(file.Name())
	}
}

func InMemoryMergePartialIndexes(indexes ...PartialIndex) PartialIndex {
	index := PartialIndex{}
	if len(indexes) == 0 {
		return index
	}
	if len(index) == 1 {
		return indexes[0]
	}

	index = indexes[0]
	for i := 1; i < len(indexes); i++ {
		for term, postings := range indexes[i] {
			index[term] = append(index[term], postings...)
		}
	}

	for term := range index {
		sort.Slice(index[term], func(i, j int) bool {
			return index[term][i].DocID < index[term][j].DocID
		})
	}

	return index
}

type PartialIndexIter struct {
	Next func() (int, InvertedListIter, bool)
	Stop func()
}

func PartialIndexIterator(file string) PartialIndexIter {
	var indexIter PartialIndexIter
	listIter := InvertedListIterator(file)
	next, stop := iter.Pull2(listIter)
	indexIter.Next = next
	indexIter.Stop = stop
	return indexIter
}

func KwayMergeReader(tempDir string, files ...string) string {
	comparator := func(a, b string) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		} else {
			return 0
		}
	}

	type Item struct {
		FileID   int
		ListIter InvertedListIter
	}

	// initialize
	readerQ := pq.NewWith(comparator)
	indexList := []PartialIndexIter{}
	itemMap := map[string][]Item{}
	for i, file := range files {
		indexIter := PartialIndexIterator(file)
		defer indexIter.Stop()

		indexList = append(indexList, indexIter)
		_, listIter, ok := indexIter.Next()
		if !ok {
			continue
		}
		defer listIter.Stop()

		term := listIter.Term
		if len(itemMap[term]) == 0 {
			readerQ.Enqueue(term)
		}
		itemMap[term] = append(itemMap[term], Item{
			FileID:   i,
			ListIter: listIter,
		})
	}

	out, err := os.CreateTemp(tempDir, "merge-index")
	if err != nil {
		log.Fatalf("failed to create temp file: %v\n", err)
	}
	bw := NewByteWriter(NewBufferedWriteCloser(out))

	for !readerQ.Empty() {
		term, ok := readerQ.Dequeue()
		if !ok {
			break
		}

		items := itemMap[term]
		listIters := []InvertedListIter{}
		for _, item := range items {
			listIters = append(listIters, item.ListIter)
		}
		err := KwayMergeWriter(bw, listIters)
		if err != nil {
			log.Fatalf("failed to merge inverted list: %v\n", err)
		}

		for _, item := range items {
			_, listIter, ok := indexList[item.FileID].Next()
			if !ok {
				continue
			}
			defer listIter.Stop()

			term := listIter.Term
			if len(itemMap[term]) == 0 {
				readerQ.Enqueue(term)
			}
			itemMap[term] = append(itemMap[term], Item{
				FileID:   item.FileID,
				ListIter: listIter,
			})
		}

		itemMap[term] = []Item{}
	}

	return out.Name()
}

func KwayMergeWriter(bw *ByteWriter, listIters []InvertedListIter) error {
	if len(listIters) == 0 {
		return nil
	}

	type Item struct {
		ListID  int
		Posting Posting
	}

	comparator := func(item1, item2 Item) int {
		a := item1.Posting
		b := item2.Posting

		if a.Type < b.Type {
			return -1
		} else if a.Type > b.Type {
			return 1
		} else {
			if a.DocID < b.DocID {
				return -1
			} else if a.DocID > b.DocID {
				return 1
			} else {
				if a.Pos < b.Pos {
					return -1
				} else if a.Pos > b.Pos {
					return 1
				} else {
					return 0
				}
			}
		}
	}

	listMap := map[int]InvertedListIter{}
	writerQ := pq.NewWith(comparator)
	term := listIters[0].Term
	totalLength := 0
	for i, listIter := range listIters {
		// defer listIter.Stop()
		totalLength += listIter.Length
		listMap[i] = listIter
		_, posting, ok := listIter.Next()
		if !ok {
			continue
		}
		writerQ.Enqueue(Item{
			ListID:  i,
			Posting: posting,
		})
	}

	err := bw.WriteString(term)
	if err != nil {
		return err
	}

	err = bw.WriteInt(totalLength)
	if err != nil {
		return err
	}

	for !writerQ.Empty() {
		item, ok := writerQ.Dequeue()
		if !ok {
			break
		}
		err = WritePosting(bw, item.Posting)
		if err != nil {
			return err
		}

		_, posting, ok := listMap[item.ListID].Next()
		if !ok {
			continue
		}
		writerQ.Enqueue(Item{
			ListID:  item.ListID,
			Posting: posting,
		})
	}

	return nil
}

// func ExternalMergePartialIndex(tempDir string, workers int, producer stream.Producer, consumer stream.Consumer) {
// 	mergeWorker := func(in <-chan []string, out chan<- string) {
// 		for fileNames := range in {
// 			name1, name2 := fileNames[0], fileNames[1]
// 			f1, err := os.Open(name1)
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			f2, err := os.Open(name2)
// 			if err != nil {
// 				log.Fatal(err)
// 			}

// 			br1 := NewByteReader(f1)
// 			br2 := NewByteReader(f2)
// 			index1, err := ReadPartialIndex(br1)
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			index2, err := ReadPartialIndex(br2)
// 			if err != nil {
// 				log.Fatal(err)
// 			}

// 			outF, err := os.CreateTemp(tempDir, "partial-index")
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			bw := NewByteWriter(outF)

// 		}
// 	}

// 	sendCh := make(chan []string)
// 	recvCh := make(chan string)

// 	for range workers {
// 		go mergeWorker(sendCh, recvCh)
// 	}

// 	for {
// 		v, ok := producer.Produce()
// 		if !ok {
// 			break
// 		}

// 	}
// }
