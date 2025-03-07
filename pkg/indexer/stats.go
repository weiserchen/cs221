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
