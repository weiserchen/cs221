package indexer

import (
	"encoding/gob"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"path"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/binary"
	"petersearch/pkg/utils/stream"
	"petersearch/pkg/utils/sys"
	"petersearch/pkg/utils/units"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
)

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

	br := binary.NewBufferedByteReader(f)
	return ReadPartialIndex(br)
}

func ReadPartialIndex(br *binary.ByteReader) (PartialIndex, error) {
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

func WritePartialIndex(bw *binary.ByteWriter, index PartialIndex) error {
	invertedLists := index.SortedList()
	for _, list := range invertedLists {
		term, postings := list.Term, list.Postings
		if err := bw.WriteString(term); err != nil {
			return err
		}
		for _, posting := range postings {
			WritePosting(bw, posting)
		}
		if _, err := WritePostingHeader(bw, PostingTypeEnd, 0); err != nil {
			return err
		}
	}
	return nil
}

func BuildPartialIndex(batchSize, batchCount int, producer stream.Producer, indexConsumer, statsConsumer stream.Consumer) {
	stats := NewIndexStats()
	defer func() {
		statsConsumer.Consume(stats)
	}()

	currCount := 0
	currSize := 0
	index := PartialIndex{}
	for {
		v, ok := producer.Produce()
		if !ok {
			if currCount != 0 {
				indexConsumer.Consume(index)
			}
			break
		}

		doc := v.(parser.Doc)
		stats.AddDoc(doc)
		ParsePostings(doc, index, stats)

		currCount++
		currSize += doc.RawSize
		if currSize < batchSize && currCount < batchCount {
			continue
		}

		indexConsumer.Consume(index)
		currCount = 0
		currSize = 0
		index = PartialIndex{}
	}
}

func SavePartialIndex(dir string, producer stream.Producer, consumer stream.Consumer) {
	for {
		v, ok := producer.Produce()
		if !ok {
			break
		}

		index := v.(PartialIndex)
		file, err := os.CreateTemp(dir, "partial.index")
		if err != nil {
			log.Fatal(err)
		}

		bw := binary.NewByteWriter(file)
		if err = WritePartialIndex(bw, index); err != nil {
			log.Fatal(err)
		}

		consumer.Consume(file.Name())
	}
}

func BuildIndex(batchSize, batchCount, tasks, workers int, srcDir, dstDir string, compress bool) {
	if compress {
		log.Fatal("unsupported option")
	}

	if err := sys.CreateDir(dstDir); err != nil {
		log.Fatalf("failed to create dir: %v\n", err)
	}

	docCh := make(chan parser.Doc)
	consumer := stream.NewChannelConsumer(docCh)

	rawFiles, err := parser.ReadFiles(srcDir)
	if err != nil {
		log.Fatalf("failed to read raw files: %v\n", err)
	}

	sort.Strings(rawFiles)
	total := batchCount * tasks
	if total >= 0 {
		rawFiles = rawFiles[:total]
	}

	go func() {
		defer func() {
			close(docCh)
		}()
		err = parser.ParseDirDocs(rawFiles, workers, batchSize, batchCount, consumer)
		if err != nil {
			log.Fatalf("failed to parse docs: %v\n", err)
		}
	}()

	docProducer := stream.NewChannelProducer(docCh)
	indexCh := make(chan PartialIndex)
	indexConsumer := stream.NewChannelConsumer(indexCh)
	statsConsumer := stream.NewArrayConsumer[*IndexStats]()

	go func() {
		defer func() {
			close(indexCh)
		}()
		BuildPartialIndex(batchSize, batchCount, docProducer, indexConsumer, statsConsumer)
	}()

	tempDir := "./temp.index"
	if err = sys.CreateDir(tempDir); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	start := time.Now()
	files := []string{}
	indexCount := 0
	for index := range indexCh {
		file, err := os.CreateTemp(tempDir, "index")
		if err != nil {
			log.Fatalf("failed to create temp dir: %v\n", err)
		}
		bw := binary.NewBufferedByteWriter(file)
		if err := WritePartialIndex(bw, index); err != nil {
			log.Fatalf("failed to write partial index: %v\n", err)
		}
		files = append(files, file.Name())
		bw.Close()

		indexCount++
	}
	log.Printf("All Iters Saved: %d. Time: %v.\n", len(files), time.Since(start))
	runtime.GC()
	indexStats := statsConsumer.Collect()[0]

	indexIters := []PartialIndexIter{}
	for _, file := range files {
		indexIters = append(indexIters, FilePartialIndexIterator(file))
	}

	outFile, err := os.CreateTemp("./", "index")
	if err != nil {
		log.Fatalf("failed to create out file: %v\n", err)
	}
	defer func() {
		os.Remove(outFile.Name())
	}()

	posFile, err := os.CreateTemp("./", "pos")
	if err != nil {
		log.Fatalf("failed to create pos file: %v\n", err)
	}
	posW := binary.NewByteWriter(binary.NewBufferedWriteCloser(posFile))
	defer posW.Close()

	bufWriter := binary.NewBufferedWriteCloser(outFile)
	bw := binary.NewByteWriter(bufWriter)
	defer bw.Close()

	start = time.Now()
	outIter := KwayMergeReader(indexIters)
	postingCount := 0
	unigrams := 0
	twograms := 0
	threegrams := 0

	type TermBuf struct {
		term string
		buf  []byte
	}
	gzipCh := make(chan TermBuf, 10)
	bwCh := make(chan TermBuf, 10)
	finCh := make(chan struct{})

	go func() {
		defer close(bwCh)
		for termBuf := range gzipCh {
			if !compress {
				bwCh <- termBuf
			}
			// else {
			// 	var zipB bytes.Buffer
			// 	gz := gzip.NewWriter(&zipB)
			// 	if _, err = gz.Write(termBuf.buf); err != nil {
			// 		log.Fatalf("failed to compress term %s: %v\n", termBuf.term, err)
			// 	}
			// 	if err := gz.Close(); err != nil {
			// 		log.Fatalf("failed to compress term %s: %v\n", termBuf.term, err)
			// 	}
			// 	if _, err = bw.Write(zipB.Bytes()); err != nil {
			// 		log.Fatalf("failed to compress term %s: %v\n", termBuf.term, err)
			// 	}
			// 	termBuf.buf = zipB.Bytes()
			// 	bwCh <- termBuf
			// }
		}
	}()

	go func() {
		var pos uint64
		prevTerm := ""
		defer func() {
			close(finCh)
		}()
		for termBuf := range bwCh {
			if termBuf.term <= prevTerm {
				log.Fatalf("entry already existed or out or order: %s\n", termBuf.term)
			}
			prevTerm = termBuf.term

			if err := posW.WriteCompactString(termBuf.term); err != nil {
				log.Fatal(err)
			}
			if _, err = bw.Write(termBuf.buf); err != nil {
				log.Fatalf("failed to write term %s: %v\n", termBuf.term, err)
			}

			pos = bufWriter.Total()
			if err := posW.WriteUInt64(pos); err != nil {
				log.Fatal(err)
			}
		}
	}()

	defer outIter.Stop()
	for {
		listPostingCount := 0
		listUnigrams := 0
		listTwograms := 0
		listThreegrams := 0
		_, listIter, ok := outIter.Next()
		if !ok {
			break
		}
		plusCount := strings.Count(listIter.Term, "+")
		switch plusCount {
		case 1:
			listTwograms++
		case 2:
			listThreegrams++
		default:
			listUnigrams++
		}

		tempBuf := binary.NewMemBuf()
		tempBw := binary.NewByteWriter(tempBuf)

		if err = tempBw.WriteString(listIter.Term); err != nil {
			log.Fatalf("failed to write term in out file: %v\n", err)
		}
		for {
			_, posting, ok := listIter.Next()
			if !ok {
				break
			}
			listPostingCount++
			if err := WritePosting(tempBw, posting); err != nil {
				log.Fatalf("failed to write posting in out file: %v\n", err)
			}
		}
		if _, err := WritePostingHeader(tempBw, PostingTypeEnd, 0); err != nil {
			log.Fatalf("failed to write end mark in out file: %v\n", err)
		}

		listIter.Stop()

		// ignore uncommon terms
		if len(listIter.Term) < 65535 && listPostingCount > 1 {
			postingCount += listPostingCount
			unigrams += listUnigrams
			twograms += listTwograms
			threegrams += listThreegrams
			gzipCh <- TermBuf{
				term: listIter.Term,
				buf:  tempBuf.Bytes(),
			}
		}
	}

	close(gzipCh)
	<-finCh

	outFStats, err := outFile.Stat()
	if err != nil {
		log.Fatalf("failed to get stat of out file: %v\n", err)
	}

	statsPath := path.Join(dstDir, "term_stats")
	statsFile, err := sys.CreateFile(statsPath)
	if err != nil {
		log.Fatalf("failed to create stats file: %v\n", err)
	}
	defer statsFile.Close()

	statsEncoder := gob.NewEncoder(statsFile)
	statsEncoder.Encode(indexStats)

	statsFStats, err := statsFile.Stat()
	if err != nil {
		log.Fatalf("failed to get stat of stats file: %v\n", err)
	}

	posPath := path.Join(dstDir, "term_pos")
	os.Rename(posFile.Name(), posPath)

	posFStats, err := posFile.Stat()
	if err != nil {
		log.Fatalf("failed to get stat of pos file: %v\n", err)
	}

	log.Printf(
		"BatchSize: %d MB. BatchCount: %d. Tasks: %d. Unigram: %d. Twogram: %d. Threegram: %d. Postings: %d. Index Size: %.2f MB. Term Stats Size: %.2f MB. Pos Stats Size: %.2f MB. Time: %v\n",
		batchSize/1000_000, batchCount, tasks, unigrams, twograms, threegrams, postingCount,
		float64(outFStats.Size())/units.MB, float64(statsFStats.Size())/units.MB, float64(posFStats.Size())/units.MB,
		time.Since(start))

	oldPath := outFile.Name()
	indexPath := path.Join(dstDir, "term_list")
	os.Rename(oldPath, indexPath)
}

func ReadPosFile(posFile *os.File) PosStats {
	stats := NewPosStats()
	posR := binary.NewByteReader(binary.NewBufferedReadCloserSize(posFile, 4096))
	defer posR.Close()

	var start uint64
	for {
		term, err := posR.ReadCompactString()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		end, err := posR.ReadUInt64()
		if err != nil {
			log.Fatal(err)
		}

		stats.TermStart[term] = start
		stats.TermEnd[term] = end

		start = end
	}

	return *stats
}
