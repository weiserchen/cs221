package main

import (
	"bufio"
	"encoding/gob"
	"log"
	"os"
	"path"
	"petersearch/pkg/indexer"
	"petersearch/pkg/utils/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexIntegrity(t *testing.T) {
	batchSize := 100_000_000
	batchCount := 100
	tasks := 10
	workers := 4
	srcDir := "../../DEV"
	dstDir := "./.index"

	defer t.Cleanup(func() {
		os.RemoveAll(dstDir)
	})

	indexer.BuildIndex(batchSize, batchCount, tasks, workers, srcDir, dstDir, false)

	log.Println("index build completed...")

	indexPath := path.Join(dstDir, "term_list")
	statsPath := path.Join(dstDir, "term_stats")
	posPath := path.Join(dstDir, "term_pos")

	var indexStats indexer.IndexStats
	var posStats indexer.PosStats

	statsFile, err := os.Open(statsPath)
	require.NoError(t, err)
	statsDecoder := gob.NewDecoder(bufio.NewReader(statsFile))
	statsDecoder.Decode(&indexStats)

	posFile, err := os.Open(posPath)
	require.NoError(t, err)
	posStats = indexer.ReadPosFile(posFile)

	indexIter := indexer.FilePartialIndexIterator(indexPath)
	memoryIndex := map[string][]indexer.Posting{}
	prevTerm := ""
	for {
		_, listIter, ok := indexIter.Next()
		if !ok {
			break
		}
		require.Less(t, prevTerm, listIter.Term)

		prevPosting := ""
		for {
			_, posting, ok := listIter.Next()
			if !ok {
				break
			}
			require.Less(t, prevPosting, posting.ID())
			memoryIndex[listIter.Term] = append(memoryIndex[listIter.Term], posting)
		}
	}

	require.Equal(t, indexStats.DocCount, len(indexStats.DocIDToURL))
	require.Equal(t, indexStats.DocCount, len(indexStats.URLToDocID))

	indexFile, err := os.Open(indexPath)
	require.NoError(t, err)
	for term, pos := range posStats.TermStart {
		indexFile.Seek(int64(pos), 0)
		br := binary.NewBufferedByteReader(indexFile)
		listIter, err := indexer.ReadInvertedList(br)
		require.NoError(t, err)

		gotTerm := listIter.Term
		require.Equal(t, term, gotTerm)

		gotPostings := indexer.CollectInvertedList(listIter)
		expectedPostings := memoryIndex[term]
		require.Equal(t, expectedPostings, gotPostings)
	}

}
