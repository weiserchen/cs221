package engine

import (
	"math"
	"petersearch/pkg/indexer"
)

type TermStats struct {
	// DocID -> Term -> Count
	m map[int]map[string]int
}

func NewTermStats() *TermStats {
	return &TermStats{
		m: map[int]map[string]int{},
	}
}

func (stats *TermStats) AddTerm(docID int, term string) {
	if _, ok := stats.m[docID]; !ok {
		stats.m[docID] = map[string]int{}
	}
	stats.m[docID][term]++
}

func (stats *TermStats) AddTermCount(docID int, term string, count int) {
	if _, ok := stats.m[docID]; !ok {
		stats.m[docID] = map[string]int{}
	}
	stats.m[docID][term] += count
}

func (stats *TermStats) TermCount(docID int, term string) int {
	if _, ok := stats.m[docID]; !ok {
		return 0
	}
	return stats.m[docID][term]
}

func CollectTermStats(list indexer.InvertedList) *TermStats {
	stats := NewTermStats()
	for _, posting := range list.Postings {
		stats.AddTerm(posting.DocID, list.Term)
	}
	return stats
}

func MergeTermStats(partialStats ...*TermStats) *TermStats {
	mergedStats := NewTermStats()

	for _, stats := range partialStats {
		for docID, freqs := range stats.m {
			for term, count := range freqs {
				mergedStats.AddTermCount(docID, term, count)
			}
		}
	}

	return mergedStats
}

type Ranker struct {
	indexStats *indexer.IndexStats
}

func NewRanker(indexStats *indexer.IndexStats) *Ranker {
	return &Ranker{
		indexStats: indexStats,
	}
}

func (r *Ranker) TFIDF(terms []string, termStats *TermStats) map[int]float64 {
	// DocID -> score
	scores := map[int]float64{}

	totalDocCount := float64(r.indexStats.DocCount)
	for docID := range termStats.m {
		for _, term := range terms {
			tf := float64(termStats.TermCount(docID, term))
			termDocCount := float64(r.indexStats.TermDocFreqCount(term))
			idf := 1 + math.Log((totalDocCount+1)/(termDocCount+1))
			scores[docID] += tf * idf * idf
		}
	}

	return scores
}

func (r *Ranker) BM25(terms []string, termStats *TermStats) map[int]float64 {
	// DocID -> score
	scores := map[int]float64{}

	totalDocCount := float64(r.indexStats.DocCount)
	avgTermCount := float64(r.indexStats.TermCount()) / totalDocCount
	k, b := 1.5, 0.75
	for docID := range termStats.m {
		for _, term := range terms {
			termDocCount := float64(r.indexStats.TermDocFreqCount(term))
			idf := math.Log((totalDocCount - termDocCount + 0.5) / (termDocCount + 0.5 + 1))
			tf := float64(termStats.TermCount(docID, term))
			scores[docID] += idf * tf / (tf + k*(1-b+b*totalDocCount/avgTermCount))
		}
	}

	return scores
}
