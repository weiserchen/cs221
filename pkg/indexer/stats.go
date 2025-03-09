package indexer

import "petersearch/pkg/parser"

type IndexStats struct {
	DocCount     int
	DocIDToURL   map[uint64]string
	URLToDocID   map[string]uint64
	DocTermCount map[uint64]int
}

func NewIndexStats() *IndexStats {
	return &IndexStats{
		DocIDToURL:   map[uint64]string{},
		URLToDocID:   map[string]uint64{},
		DocTermCount: map[uint64]int{},
	}
}

func (stats *IndexStats) AddDoc(doc parser.Doc) {
	stats.DocCount++
	stats.DocIDToURL[doc.ID] = doc.URL
	stats.URLToDocID[doc.URL] = doc.ID
}

func (stats *IndexStats) AddTerm(docID uint64, term string) {
	stats.DocTermCount[docID]++
}

func (stats *IndexStats) DocLen(docID uint64) int {
	return stats.DocTermCount[docID]
}

func (stats *IndexStats) AvgTermPerDoc() float64 {
	totalTermCount := 0
	for _, count := range stats.DocTermCount {
		totalTermCount += count
	}
	return float64(totalTermCount) / float64(stats.DocCount)
}

type PosStats struct {
	TermStart map[string]uint64
	TermEnd   map[string]uint64
}

func NewPosStats() *PosStats {
	return &PosStats{
		TermStart: map[string]uint64{},
		TermEnd:   map[string]uint64{},
	}
}
