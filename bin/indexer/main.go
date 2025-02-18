package main

import (
	"log"
	"os"
	"petersearch/pkg/indexer"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"time"
)

func readParsedFiles(workers, total, batch int) ([]indexer.PartialIndex, indexer.DocStats) {
	srcDir := "../../DEV"
	consumer := stream.NewArrayConsumer[parser.Doc]()

	rawFiles, err := parser.ReadFiles(srcDir)
	if err != nil {
		log.Fatal(err)
	}

	sort.Strings(rawFiles)
	if total >= 0 {
		rawFiles = rawFiles[:total]
	}

	err = parser.ParseDirDocs(rawFiles, workers, consumer)
	if err != nil {
		log.Fatal(err)
	}

	docs := consumer.Collect()
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].ID < docs[j].ID
	})

	docProducer := stream.NewArrayProducer(docs)
	indexConsumer := stream.NewArrayConsumer[indexer.PartialIndex]()
	statsConsumer := stream.NewArrayConsumer[*indexer.DocStats]()
	waitCh := make(chan struct{})

	go func() {
		defer func() {
			close(waitCh)
		}()
		indexer.BuildPartialIndex(batch, docProducer, indexConsumer, statsConsumer)
	}()

	<-waitCh

	indexes := indexConsumer.Collect()
	stats := statsConsumer.Collect()[0]
	return indexes, *stats
}

const MB = 1000.0 * 1000.0

func printMemoryUsed() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	log.Printf("Memory used: %.2f MB.\n", float64(memStats.Alloc)/MB)
}

func main() {
	traceF, err := os.Create("trace.out")
	if err != nil {
		log.Fatal(err)
	}
	err = trace.Start(traceF)
	if err != nil {
		log.Fatal(err)
	}
	defer trace.Stop()

	batch := 100
	tasks := 10
	partialIndexes, _ := readParsedFiles(4, batch*tasks, batch)
	// partialIndexes, _ := readParsedFiles(4, -1, batch)
	// partialIndexes, _ := testParseFiles(t, 1, 4, 1)
	// inMemoryIndex := indexer.InMemoryMergePartialIndexes(partialIndexes...)
	// indexIters := []indexer.PartialIndexIter{}
	// for _, partialIndex := range partialIndexes {
	// 	indexIters = append(indexIters, partialIndex.SortedIter())
	// }

	tempDir := "./temp-index"
	_, err = os.Stat("./temp-index")
	if os.IsNotExist(err) {
		err := os.Mkdir(tempDir, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer os.RemoveAll(tempDir)

	printMemoryUsed()
	start := time.Now()
	files := []string{}
	for _, index := range partialIndexes {
		file, err := os.CreateTemp(tempDir, "temp-index")
		if err != nil {
			log.Fatal(err)
		}
		bw := indexer.NewByteWriter(indexer.NewBufferedWriteCloser(file))
		if err := indexer.WritePartialIndex(bw, index); err != nil {
			log.Fatal("failed to write partial index:", err)
		}
		files = append(files, file.Name())
		bw.Close()
	}
	log.Printf("All Iters Saved: %d. Time: %v.\n", len(files), time.Since(start))
	partialIndexes = nil
	runtime.GC()
	printMemoryUsed()

	indexIters := []indexer.PartialIndexIter{}
	for _, file := range files {
		indexIters = append(indexIters, indexer.FilePartialIndexIterator(file))
	}

	file, err := os.CreateTemp("./", "index")
	if err != nil {
		log.Fatal("failed to create out file", err)
	}
	defer func() {
		os.Remove(file.Name())
	}()

	bufWriter := indexer.NewBufferedWriteCloser(file)
	bw := indexer.NewByteWriter(bufWriter)
	defer bw.Close()
	start = time.Now()
	// invertedLists := []indexer.InvertedList{}
	outIter := indexer.KwayMergeReader(indexIters)
	postingCount := 0
	listCount := 0
	defer outIter.Stop()
	for {
		if listCount%10000 == 0 {
			printMemoryUsed()
			// log.Println("bw buffered:", bufWriter.Buffered())
			// runtime.GC()
		}
		_, listIter, ok := outIter.Next()
		if !ok {
			break
		}
		defer listIter.Stop()
		listCount++

		if bw.WriteString(listIter.Term); err != nil {
			log.Fatal(err)
		}

		// postings := []indexer.Posting{}
		for {
			_, posting, ok := listIter.Next()
			if !ok {
				break
			}
			postingCount++
			if err := indexer.WritePosting(bw, posting); err != nil {
				log.Fatal(err)
			}
			// postings = append(postings, posting)
		}

		// invertedLists = append(invertedLists, indexer.InvertedList{
		// 	Term:     listIter.Term,
		// 	Postings: postings,
		// })
	}

	fi, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"Batch: %d. Tasks: %d. Total Postings: %d. Size: %.2f MB. Time: %v\n",
		batch, tasks, postingCount, float64(fi.Size())/MB, time.Since(start))

	memF, err := os.Create("mem.prof")
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer memF.Close()

	if err := pprof.WriteHeapProfile(memF); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}
