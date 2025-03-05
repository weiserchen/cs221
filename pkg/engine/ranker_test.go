package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocStats(t *testing.T) {
	srcDir := "../../.index"
	cacheSize := 256
	workers := 4

	ng := NewEngine(srcDir, cacheSize, workers)
	totalStats := NewDocStats()
	batch := 100
	tasks := 10
	limit := batch * tasks
	count := 0
	terms := []string{}
	for term := range ng.TermPos {
		if count >= limit {
			break
		}
		terms = append(terms, term)
		list, err := ng.Cache.Get(term)
		require.NoError(t, err)
		for _, posting := range list.Postings {
			totalStats.AddTerm(posting.DocID, term)
		}
		count++
	}

	docStats := make([]*DocStats, 0, tasks)
	for l := range tasks {
		stats := NewDocStats()
		for b := range batch {
			term := terms[l*batch+b]
			list, err := ng.Cache.Get(term)
			require.NoError(t, err)
			for _, posting := range list.Postings {
				stats.AddTerm(posting.DocID, term)
			}
		}
		docStats = append(docStats, stats)
	}
	mergedStats := MergeDocStats(docStats...)

	require.Equal(t, len(totalStats.TermFreqs), len(mergedStats.TermFreqs))
	require.Equal(t, len(totalStats.DocFreqs), len(mergedStats.DocFreqs))

	for docID, expectedFreqs := range totalStats.TermFreqs {
		gotFreqs, ok := mergedStats.TermFreqs[docID]
		require.True(t, ok)
		require.Equal(t, len(expectedFreqs), len(gotFreqs))
		for term, expectedCount := range expectedFreqs {
			gotCount := gotFreqs[term]
			require.Equal(t, expectedCount, gotCount)
		}
	}

	for term, expectedFreqs := range totalStats.DocFreqs {
		gotFreqs, ok := mergedStats.DocFreqs[term]
		require.True(t, ok)
		require.Equal(t, len(expectedFreqs), len(gotFreqs))
		for docID := range expectedFreqs {
			_, ok = gotFreqs[docID]
			require.True(t, ok)
		}
	}
}
