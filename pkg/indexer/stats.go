package indexer

import "petersearch/pkg/parser"

type IndexStats struct {
	DocCount     int
	DocIDToURL   map[int]string
	URLToDocID   map[string]int
	DocTermCount map[int]int
	// term -> doc -> ok
	TermDocCount map[string]map[int]bool
}

func NewIndexStats() *IndexStats {
	return &IndexStats{
		DocIDToURL:   map[int]string{},
		URLToDocID:   map[string]int{},
		DocTermCount: map[int]int{},
		TermDocCount: map[string]map[int]bool{},
	}
}

func (stats *IndexStats) AddDoc(doc parser.Doc) {
	stats.DocCount++
	stats.DocIDToURL[doc.ID] = doc.URL
	stats.URLToDocID[doc.URL] = doc.ID
}

func (stats *IndexStats) AddTerm(docID int, term string) {
	if _, ok := stats.TermDocCount[term]; !ok {
		stats.TermDocCount[term] = map[int]bool{}
	}
	stats.TermDocCount[term][docID] = true
	stats.DocTermCount[docID]++
}

func (stats *IndexStats) TermCount() int {
	return len(stats.TermDocCount)
}

func (stats *IndexStats) TermDocFreqCount(term string) int {
	if _, ok := stats.TermDocCount[term]; !ok {
		return 0
	}
	return len(stats.TermDocCount[term])
}

func (stats *IndexStats) DocLen(docID int) int {
	return stats.DocTermCount[docID]
}

type DocStats struct {
	DocID     int
	TermFreqs map[string]int
}

func NewDocStats(docID int) *DocStats {
	return &DocStats{
		DocID:     docID,
		TermFreqs: map[string]int{},
	}
}

func (stats *DocStats) AddTerm(term string) {
	stats.TermFreqs[term]++
}

func (stats *DocStats) Freq(term string) int {
	return stats.TermFreqs[term]
}
