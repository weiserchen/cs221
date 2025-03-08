package engine

import (
	"encoding/gob"
	"os"
	"path"
	"petersearch/pkg/indexer"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	batch := 100
	tasks := 10
	workers := 4
	srcDir := "../../DEV"
	dstDir := "./cache.index"

	t.Cleanup(func() {
		os.RemoveAll(dstDir)
	})

	compress := false
	indexer.BuildIndex(batch, tasks, workers, srcDir, dstDir, compress)

	indexPath := path.Join(dstDir, "term_list")
	statsPath := path.Join(dstDir, "term_stats")
	posPath := path.Join(dstDir, "term_pos")

	var indexStats indexer.IndexStats
	var posStats indexer.PosStats

	statsFile, err := os.Open(statsPath)
	require.NoError(t, err)
	statsDecoder := gob.NewDecoder(statsFile)
	statsDecoder.Decode(&indexStats)

	posFile, err := os.Open(posPath)
	require.NoError(t, err)
	posDecoder := gob.NewDecoder(posFile)
	posDecoder.Decode(&posStats)

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

	memCacheSize := 256
	diskCache := NewDiskIndexListCache(indexPath, 4, posStats, compress)
	memCache := NewMemoryIndexListCache(memCacheSize, diskCache)

	nonExistTerm := "areallylongtermthatdoesnotexistinthecache"

	_, err = memCache.Get(nonExistTerm)
	require.Error(t, err)
	require.Equal(t, ErrCacheEntryNotFound, err)

	var wg sync.WaitGroup
	wg.Add(len(memoryIndex))
	for term, postings := range memoryIndex {
		go func() {
			defer wg.Done()
			list, err := memCache.Get(term)
			require.NoError(t, err)
			require.Equal(t, term, list.Term)
			require.Equal(t, postings, list.Postings)
		}()
	}
	wg.Wait()
}

func TestCacheCompressed(t *testing.T) {
	batch := 100
	tasks := 10
	workers := 4
	srcDir := "../../DEV"
	dstDir := "./cache.index"

	t.Cleanup(func() {
		os.RemoveAll(dstDir)
	})

	compress := true
	indexer.BuildIndex(batch, tasks, workers, srcDir, dstDir, compress)

	indexPath := path.Join(dstDir, "term_list")
	statsPath := path.Join(dstDir, "term_stats")
	posPath := path.Join(dstDir, "term_pos")

	var indexStats indexer.IndexStats
	var posStats indexer.PosStats

	statsFile, err := os.Open(statsPath)
	require.NoError(t, err)
	statsDecoder := gob.NewDecoder(statsFile)
	statsDecoder.Decode(&indexStats)

	posFile, err := os.Open(posPath)
	require.NoError(t, err)
	posDecoder := gob.NewDecoder(posFile)
	posDecoder.Decode(&posStats)

	memCacheSize := 256
	diskCache := NewDiskIndexListCache(indexPath, 4, posStats, compress)
	memCache := NewMemoryIndexListCache(memCacheSize, diskCache)

	nonExistTerm := "areallylongtermthatdoesnotexistinthecache"

	_, err = memCache.Get(nonExistTerm)
	require.Error(t, err)
	require.Equal(t, ErrCacheEntryNotFound, err)

	for term := range posStats.TermStart {
		_, err := memCache.Get(term)
		require.NoError(t, err)
	}
}
