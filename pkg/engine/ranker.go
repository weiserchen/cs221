package engine

import (
	"math"
	"petersearch/pkg/indexer"
	"slices"
	"strings"
)

type RankAlgo int

const (
	RankAlgoTFIDF RankAlgo = iota
	RankAlgoBM25
)

type DocStats struct {
	// DocID -> Term -> Weighted Count
	TermFreqs map[uint64]map[string]float64
	// Term -> DocID -> ok
	DocFreqs map[string]map[uint64]struct{}
}

func NewDocStats() *DocStats {
	return &DocStats{
		TermFreqs: map[uint64]map[string]float64{},
		DocFreqs:  map[string]map[uint64]struct{}{},
	}
}

func (stats *DocStats) AddPosting(term string, posting indexer.Posting) {
	docID := posting.DocID
	if _, ok := stats.TermFreqs[docID]; !ok {
		stats.TermFreqs[docID] = map[string]float64{}
	}

	weightedCount := 0.0
	switch posting.Type {
	case indexer.PostingTypeTag:
		switch posting.Tag {
		case "title":
			weightedCount += 5
		case "h1":
			weightedCount += 3
		case "h2":
			weightedCount += 2
		case "h3":
			weightedCount += 1
		case "b", "i":
			weightedCount += 0.5
		}
	}

	plusCount := strings.Count(term, "+")
	if plusCount == 0 {
		stats.TermFreqs[docID][term] += weightedCount
	} else {
		stats.TermFreqs[docID][term] += weightedCount * math.Pow(2, float64(plusCount))
	}

	if _, ok := stats.DocFreqs[term]; !ok {
		stats.DocFreqs[term] = map[uint64]struct{}{}
	}
	stats.DocFreqs[term][docID] = struct{}{}
}

func (stats *DocStats) AddTermWeightedCount(docID uint64, term string, count float64) {
	if _, ok := stats.TermFreqs[docID]; !ok {
		stats.TermFreqs[docID] = map[string]float64{}
	}
	stats.TermFreqs[docID][term] += count
}

func (stats *DocStats) AddDocFreq(docID uint64, term string) {
	if _, ok := stats.DocFreqs[term]; !ok {
		stats.DocFreqs[term] = map[uint64]struct{}{}
	}
	stats.DocFreqs[term][docID] = struct{}{}
}

func (stats *DocStats) TermWeightedCount(docID uint64, term string) float64 {
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
		stats.AddPosting(list.Term, posting)
	}
	return stats
}

func MergeDocStats(docStats ...*DocStats) *DocStats {
	mergedStats := NewDocStats()

	for _, stats := range docStats {
		for docID, freqs := range stats.TermFreqs {
			for term, count := range freqs {
				mergedStats.AddTermWeightedCount(docID, term, count)
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
			termCount := docStats.TermWeightedCount(docID, term)
			termDocCount := float64(docStats.DocCount(term))
			tf := termCount / docLen
			idf := 1 + math.Log((totalDocCount+1)/(termDocCount+1))
			// scoreMap[docID] += tf * idf * idf
			scoreMap[docID] += tf * idf
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

func (r *Ranker) BM25(terms []string, docStats *DocStats) []Score {
	// DocID -> score
	scoreMap := map[uint64]float64{}

	k, b := 1.5, 0.75
	avg := r.indexStats.AvgTermPerDoc()
	N := float64(r.indexStats.DocCount)
	for docID := range docStats.TermFreqs {
		d := float64(r.indexStats.DocLen(docID))
		for _, term := range terms {
			tf := docStats.TermWeightedCount(docID, term)
			Nt := float64(docStats.DocCount(term))
			idf := math.Log((N-Nt+0.5)/(Nt+0.5) + 1)
			scoreMap[docID] += idf * tf / (tf + k*(1-b+b*d/avg))
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
