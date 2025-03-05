package indexer

import "petersearch/pkg/parser"

type IndexStats struct {
	DocCount     int
	DocIDToURL   map[int]string
	URLToDocID   map[string]int
	DocTermCount map[int]int
}

func NewIndexStats() *IndexStats {
	return &IndexStats{
		DocIDToURL:   map[int]string{},
		URLToDocID:   map[string]int{},
		DocTermCount: map[int]int{},
	}
}

func (stats *IndexStats) AddDoc(doc parser.Doc) {
	stats.DocCount++
	stats.DocIDToURL[doc.ID] = doc.URL
	stats.URLToDocID[doc.URL] = doc.ID
}

func (stats *IndexStats) AddTerm(docID int, term string) {
	stats.DocTermCount[docID]++
}

func (stats *IndexStats) DocLen(docID int) int {
	return stats.DocTermCount[docID]
}
