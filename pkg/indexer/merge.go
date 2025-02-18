package indexer

import (
	"iter"
	"log"
	"os"
	"petersearch/pkg/utils/stream"
	"slices"

	pq "github.com/emirpasic/gods/v2/queues/priorityqueue"
)

func InMemoryMergePartialIndexes(indexes ...PartialIndex) PartialIndex {
	index := PartialIndex{}
	if len(indexes) == 0 {
		return index
	}
	if len(index) == 1 {
		return indexes[0]
	}

	for i := 0; i < len(indexes); i++ {
		for term, postings := range indexes[i] {
			index[term] = append(index[term], postings...)
		}
	}

	for term, postings := range index {
		slices.SortFunc(postings, SortPostingsComparator())
		index[term] = postings
	}

	return index
}

func KwayMergeReader(indexIters []PartialIndexIter) PartialIndexIter {
	type Item struct {
		ID       int
		ListIter InvertedListIter
	}

	comparator := func(a, b string) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		} else {
			return 0
		}
	}

	iterFunc := func(yield func(int, InvertedListIter) bool) {
		defer func() {
			for _, indexIter := range indexIters {
				indexIter.Stop()
			}
		}()

		readerQ := pq.NewWith(comparator)
		itemMap := map[string][]Item{}
		for i, indexIter := range indexIters {
			_, listIter, ok := indexIter.Next()
			if !ok {
				continue
			}

			term := listIter.Term
			if len(itemMap[term]) == 0 {
				readerQ.Enqueue(term)
			}
			itemMap[term] = append(itemMap[term], Item{
				ID:       i,
				ListIter: listIter,
			})
		}

		count := 0
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
			outListIter := KwayMergeWriter(listIters)
			// outList := KwayMergeWriterCollector(listIters)
			// outListIter := InMemoryInvertedListIterator(outList)

			if !yield(count, outListIter) {
				break
			}
			count++
			delete(itemMap, term)

			for _, item := range items {
				_, listIter, ok := indexIters[item.ID].Next()
				if !ok {
					continue
				}

				term := listIter.Term
				if len(itemMap[term]) == 0 {
					readerQ.Enqueue(term)
				}
				itemMap[term] = append(itemMap[term], Item{
					ID:       item.ID,
					ListIter: listIter,
				})
			}
		}
	}

	var outIter PartialIndexIter
	next, stop := iter.Pull2(iterFunc)
	outIter.Next = next
	outIter.Stop = stop

	return outIter
}

func KwayMergeWriterCollector(listIters []InvertedListIter) InvertedList {
	var list InvertedList
	list.Term = listIters[0].Term

	for _, listIter := range listIters {
		list.Postings = append(list.Postings, CollectInvertedList(listIter)...)
	}
	slices.SortFunc(list.Postings, SortPostingsComparator())

	return list
}

func KwayMergeWriter(listIters []InvertedListIter) InvertedListIter {
	type Item struct {
		ID      int
		Posting Posting
	}

	comparator := func(item1, item2 Item) int {
		return SortPostingsComparator()(item1.Posting, item2.Posting)
	}

	iterFunc := func(yield func(int, Posting) bool) {
		if len(listIters) == 0 {
			return
		}

		listMap := map[int]InvertedListIter{}
		writerQ := pq.NewWith(comparator)
		for i, listIter := range listIters {
			defer listIter.Stop()
			listMap[i] = listIter
			_, posting, ok := listIter.Next()
			if !ok {
				continue
			}
			writerQ.Enqueue(Item{
				ID:      i,
				Posting: posting,
			})
		}

		count := 0
		for !writerQ.Empty() {
			item, ok := writerQ.Dequeue()
			if !ok {
				break
			}

			// log.Println(item.Posting)
			if !yield(count, item.Posting) {
				break
			}
			count++

			_, posting, ok := listMap[item.ID].Next()
			if !ok {
				continue
			}
			writerQ.Enqueue(Item{
				ID:      item.ID,
				Posting: posting,
			})
		}
	}

	var outIter InvertedListIter
	if len(listIters) > 0 {
		outIter.Term = listIters[0].Term
	}

	next, stop := iter.Pull2(iterFunc)
	outIter.Next = next
	outIter.Stop = stop

	return outIter
}

func MergePartialIndex(dir string, producer stream.Producer, consumer stream.Consumer) {
	for {
		v, ok := producer.Produce()
		if !ok {
			break
		}

		files := v.([]string)
		indexIters := []PartialIndexIter{}
		for _, file := range files {
			indexIters = append(indexIters, FilePartialIndexIterator(file))
		}

		iterFunc := func() string {
			outFile, err := os.CreateTemp(dir, "merge-index")
			if err != nil {
				log.Fatalf("failed to create temp file: %v\n", err)
			}
			defer outFile.Close()

			bw := NewByteWriter(NewBufferedWriteCloser(outFile))

			outIter := KwayMergeReader(indexIters)
			defer outIter.Stop()
			for {
				_, listIter, ok := outIter.Next()
				if !ok {
					break
				}
				defer listIter.Stop()

				if err := bw.WriteString(listIter.Term); err != nil {
					log.Fatalf("failed to write term %s: %v\n", listIter.Term, err)
				}

				for {
					_, posting, ok := listIter.Next()
					if !ok {
						break
					}
					if err := WritePosting(bw, posting); err != nil {
						log.Fatalf("failed to write posting %v: %v\n", posting, err)
					}
				}
			}

			return outFile.Name()
		}

		outName := iterFunc()
		consumer.Consume(outName)
	}
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
