package indexer

import (
	"fmt"
	"os"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/binary"
	"petersearch/pkg/utils/stream"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func testParseFiles(t *testing.T, workers, total, batch int) ([]PartialIndex, IndexStats) {
	t.Helper()

	srcDir := "../../DEV"
	consumer := stream.NewArrayConsumer[parser.Doc]()

	rawFiles, err := parser.ReadFiles(srcDir)
	require.NoError(t, err)

	sort.Strings(rawFiles)
	rawFiles = rawFiles[:total]

	err = parser.ParseDirDocs(rawFiles, workers, consumer)
	require.NoError(t, err)

	docs := consumer.Collect()
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].ID < docs[j].ID
	})

	docProducer := stream.NewArrayProducer(docs)
	indexConsumer := stream.NewArrayConsumer[PartialIndex]()
	statsConsumer := stream.NewArrayConsumer[*IndexStats]()
	waitCh := make(chan struct{})

	go func() {
		defer func() {
			close(waitCh)
		}()
		BuildPartialIndex(batch, docProducer, indexConsumer, statsConsumer)
	}()

	<-waitCh

	indexes := indexConsumer.Collect()
	stats := statsConsumer.Collect()[0]
	return indexes, *stats
}

func TestUniquePostings(t *testing.T) {
	partialIndexes, _ := testParseFiles(t, 1, 1000, 10)
	set := map[string]bool{}
	for _, index := range partialIndexes {
		for _, postings := range index {
			for _, posting := range postings {
				postingID := posting.ID()
				_, ok := set[postingID]
				require.False(t, ok)
				set[postingID] = true
			}
		}
	}
}

func TestBinaryReadWrite(t *testing.T) {
	partialIndexes, docStats := testParseFiles(t, 1, 100, 10)

	terms := []string{}
	for term := range partialIndexes[0] {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		postings := partialIndexes[0][term]
		fmt.Println(term, postings)
	}
	fmt.Println(docStats.DocCount)

	tempDir := "./bin-rw"
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		err := os.Mkdir(tempDir, 0755)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	for _, index := range partialIndexes {
		file, err := os.CreateTemp(tempDir, "bin")
		fileName := file.Name()
		require.NoError(t, err)
		bw := binary.NewBufferedByteWriter(file)
		WritePartialIndex(bw, index)
		bw.Close()

		f, err := os.Open(fileName)
		require.NoError(t, err)
		br := binary.NewBufferedByteReader(f)
		storedIndex, err := ReadPartialIndex(br)
		require.NoError(t, err)

		require.Equal(t, len(index), len(storedIndex))
		for term, postings := range index {
			storedPostings, ok := storedIndex[term]
			require.True(t, ok)
			require.Equal(t, len(postings), len(storedPostings))
			for i := 0; i < len(postings); i++ {
				require.Equal(t, postings[i].Type, storedPostings[i].Type)
				require.Equal(t, postings[i].DocID, storedPostings[i].DocID)
				require.Equal(t, postings[i].Tag, storedPostings[i].Tag)
				require.Equal(t, postings[i].Pos, storedPostings[i].Pos)
			}
		}
	}
}

func TestKwayMergeReaderWriter(t *testing.T) {
	batch := 10
	tasks := 10
	partialIndexes, _ := testParseFiles(t, 1, batch*tasks, batch)
	// partialIndexes, _ := testParseFiles(t, 1, 4, 1)
	inMemoryIndex := InMemoryMergePartialIndexes(partialIndexes...)

	indexIters := []PartialIndexIter{}
	for _, partialIndex := range partialIndexes {
		indexIters = append(indexIters, partialIndex.SortedIter())
	}

	invertedLists := []InvertedList{}
	outIter := KwayMergeReader(indexIters)
	defer outIter.Stop()
	for {
		_, listIter, ok := outIter.Next()
		if !ok {
			break
		}
		defer listIter.Stop()

		postings := []Posting{}
		for {
			_, posting, ok := listIter.Next()
			if !ok {
				break
			}
			postings = append(postings, posting)
		}

		invertedLists = append(invertedLists, InvertedList{
			Term:     listIter.Term,
			Postings: postings,
		})
	}

	set := map[string]bool{}
	for _, list := range invertedLists {
		for _, posting := range list.Postings {
			postingID := posting.ID()
			_, ok := set[postingID]
			require.False(t, ok)
			set[postingID] = true
		}
	}

	require.Equal(t, len(inMemoryIndex), len(invertedLists))
	prevTerm := ""
	for _, list := range invertedLists {
		require.Greater(t, list.Term, prevTerm)
		_, ok := inMemoryIndex[list.Term]
		require.True(t, ok)
		prevTerm = list.Term
	}
	for i, list := range invertedLists {
		expectedPostings, ok := inMemoryIndex[list.Term]
		require.True(t, ok)
		require.Equal(t, len(expectedPostings), len(list.Postings),
			fmt.Sprintf("%d, %s: %v, %v", i, list.Term, expectedPostings, list.Postings))

		for i, posting := range list.Postings {
			require.Equal(t, expectedPostings[i], posting,
				fmt.Sprintf("%s: %v, %v", list.Term, expectedPostings, list.Postings))
		}
	}

	tempDir := "./temp-index"
	_, err := os.Stat("./temp-index")
	if os.IsNotExist(err) {
		err := os.Mkdir(tempDir, 0755)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	file, err := os.CreateTemp(tempDir, "index")
	require.NoError(t, err)
	bw := binary.NewBufferedByteWriter(file)
	err = WritePartialIndex(bw, inMemoryIndex)
	require.NoError(t, err)
	bw.Close()

	// f, err := os.Open(file.Name())
	// require.NoError(t, err)
	// br := NewByteReader(NewBufferedReadCloser(f))
	// savedIndex, err := ReadPartialIndex(br)
	// require.NoError(t, err)
	// savedList := savedIndex.SortedList()
	savedList := []InvertedList{}
	fileIter := FilePartialIndexIterator(file.Name())
	for {
		_, listIter, ok := fileIter.Next()
		if !ok {
			break
		}

		postings := []Posting{}
		for {
			_, posting, ok := listIter.Next()
			if !ok {
				break
			}
			postings = append(postings, posting)
		}
		savedList = append(savedList, InvertedList{
			Term:     listIter.Term,
			Postings: postings,
		})
	}

	require.Equal(t, len(invertedLists), len(savedList))
	for i := 0; i < len(savedList); i++ {
		require.Equal(t, invertedLists[i].Term, savedList[i].Term)
		require.Equal(t, len(invertedLists[i].Postings), len(savedList[i].Postings))
		for j := 0; j < len(savedList[i].Postings); j++ {
			require.Equal(t, invertedLists[i].Postings[j], savedList[i].Postings[j])
		}
	}
}

func TestMergePartialIndexes(t *testing.T) {
	// partialIndexes, _ := testParseFiles(t, 1, 100, 10)
	// inMemoryIndex := InMemoryMergePartialIndexes(partialIndexes...)
	// tempDir := "./merge-partial"
	// indexProducer := stream.NewArrayProducer(partialIndexes)
	// indexConsumer := stream.NewArrayConsumer[string]()
	// MergePartialIndex(tempDir, indexProducer, indexConsumer)

	// outFile := indexConsumer.Collect()[0]
	// outDiskIndex, err := ReadFilePartialIndex(outFile)
	// require.NoError(t, err)

}
