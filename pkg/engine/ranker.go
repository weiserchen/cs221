package engine

import (
	"math"
	"petersearch/pkg/indexer"
	"slices"
)

type DocStats struct {
	// DocID -> Term -> Count
	TermFreqs map[uint64]map[string]int
	// Term -> DocID -> ok
	DocFreqs map[string]map[uint64]struct{}
}

func NewDocStats() *DocStats {
	return &DocStats{
		TermFreqs: map[uint64]map[string]int{},
		DocFreqs:  map[string]map[uint64]struct{}{},
	}
}

func (stats *DocStats) AddTerm(docID uint64, term string) {
	if _, ok := stats.TermFreqs[docID]; !ok {
		stats.TermFreqs[docID] = map[string]int{}
	}
	stats.TermFreqs[docID][term]++

	if _, ok := stats.DocFreqs[term]; !ok {
		stats.DocFreqs[term] = map[uint64]struct{}{}
	}
	stats.DocFreqs[term][docID] = struct{}{}
}

func (stats *DocStats) AddTermCount(docID uint64, term string, count int) {
	if _, ok := stats.TermFreqs[docID]; !ok {
		stats.TermFreqs[docID] = map[string]int{}
	}
	stats.TermFreqs[docID][term] += count
}

func (stats *DocStats) AddDocFreq(docID uint64, term string) {
	if _, ok := stats.DocFreqs[term]; !ok {
		stats.DocFreqs[term] = map[uint64]struct{}{}
	}
	stats.DocFreqs[term][docID] = struct{}{}
}

func (stats *DocStats) TermCount(docID uint64, term string) int {
	if _, ok := stats.TermFreqs[docID]; !ok {
		return 0
	}
	return stats.TermFreqs[docID][term]
}

func (stats *DocStats) DocCount(term string) int {
	return len(stats.DocFreqs[term])
}

func CollectDocStats(list indexer.InvertedList) *DocStats {
	stats := NewDocStats()
	for _, posting := range list.Postings {
		stats.AddTerm(posting.DocID, list.Term)
	}
	return stats
}

func MergeDocStats(docStats ...*DocStats) *DocStats {
	mergedStats := NewDocStats()

	for _, stats := range docStats {
		for docID, freqs := range stats.TermFreqs {
			for term, count := range freqs {
				mergedStats.AddTermCount(docID, term, count)
			}
		}
		for term, freqs := range stats.DocFreqs {
			for docID := range freqs {
				mergedStats.AddDocFreq(docID, term)
			}
		}
	}

	return mergedStats
}

type Score struct {
	DocID uint64
	Value float64
}

type Ranker struct {
	indexStats *indexer.IndexStats
}

func NewRanker(indexStats *indexer.IndexStats) *Ranker {
	return &Ranker{
		indexStats: indexStats,
	}
}

func (r *Ranker) TFIDF(terms []string, docStats *DocStats) []Score {
	// DocID -> score
	scoreMap := map[uint64]float64{}

	totalDocCount := float64(r.indexStats.DocCount)
	for docID := range docStats.TermFreqs {
		docLen := float64(r.indexStats.DocLen(docID))
		for _, term := range terms {
			termCount := float64(docStats.TermCount(docID, term))
			termDocCount := float64(docStats.DocCount(term))
			tf := termCount / docLen
			idf := 1 + math.Log((totalDocCount+1)/(termDocCount+1))
			scoreMap[docID] += tf * idf * idf
			// scoreMap[docID] += tf * idf
		}
	}

	scores := make([]Score, 0, len(scoreMap))
	for docID, value := range scoreMap {
		scores = append(scores, Score{
			DocID: docID,
			Value: value,
		})
	}

	return scores
}

// func (r *Ranker) BM25(terms []string, docStats *DocStats) map[int]float64 {
// 	// DocID -> score
// 	scores := map[int]float64{}

// 	totalDocCount := float64(r.indexStats.DocCount)
// 	avgTermCount := float64(r.indexStats.TermCount()) / totalDocCount
// 	k, b := 1.5, 0.75
// 	for docID := range docStats.TermFreqs {
// 		for _, term := range terms {
// 			termDocCount := float64(r.indexStats.TermDocFreqCount(term))
// 			idf := math.Log((totalDocCount - termDocCount + 0.5) / (termDocCount + 0.5 + 1))
// 			tf := float64(docStats.TermCount(docID, term))
// 			scores[docID] += idf * tf / (tf + k*(1-b+b*totalDocCount/avgTermCount))
// 		}
// 	}

// 	return scores
// }

func SortScoreAscendComparator(s1, s2 Score) int {
	if s1.Value < s2.Value {
		return -1
	} else if s1.Value > s2.Value {
		return 1
	} else {
		return 0
	}
}

func SortScoreDescendComparator(s1, s2 Score) int {
	return -SortScoreAscendComparator(s1, s2)
}

func TopK(scores []Score, k int) []Score {
	slices.SortFunc(scores, SortScoreDescendComparator)
	if k <= 0 || len(scores) < k {
		return scores
	}
	return scores[:k]
}
