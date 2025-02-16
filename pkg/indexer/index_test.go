package indexer

import (
	"fmt"
	"os"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryReadWrite(t *testing.T) {
	srcDir := "../../DEV"
	workers := 1
	consumer := stream.NewArrayConsumer[parser.Doc]()

	rawFiles, err := parser.ReadFiles(srcDir)
	require.NoError(t, err)

	// For testing
	sort.Strings(rawFiles)
	count := 1000
	rawFiles = rawFiles[:count]

	err = parser.ParseDirDocs(rawFiles, workers, consumer)
	require.NoError(t, err)

	docs := consumer.Collect()
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].ID < docs[j].ID
	})

	batch := 100
	docProducer := stream.NewArrayProducer(docs)
	indexConsumer := stream.NewArrayConsumer[PartialIndex]()
	statsConsumer := stream.NewArrayConsumer[*DocStats]()
	waitCh := make(chan struct{})

	go func() {
		defer func() {
			close(waitCh)
		}()
		BuildPartialIndex(batch, docProducer, indexConsumer, statsConsumer)
	}()

	<-waitCh
	docStats := statsConsumer.Collect()[0]
	partialIndexes := indexConsumer.Collect()

	// require.Equal(t, count, docStats.DocCount)
	// require.Equal(t, count/batch, len(allInvertedLists))
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
	// for _, f := range rawFiles {
	// 	fmt.Println(f)
	// }
	// for _, doc := range docs {
	// 	fmt.Println(doc.ID)
	// }

	tempDir := "./testdir"
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
		bw := NewByteWriter(NewBufferedWriteCloser(file))
		WritePartialIndex(index, bw)
		bw.Close()

		f, err := os.Open(fileName)
		require.NoError(t, err)
		br := NewByteReader(NewBufferedReadCloser(f))
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
