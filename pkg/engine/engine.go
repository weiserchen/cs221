package engine

import (
	"encoding/gob"
	"log"
	"os"
	"path"
	"petersearch/pkg/indexer"
	"petersearch/pkg/parser"
)

type ResultDoc struct {
	DocID  int
	DocURL string
	Score  float64
}

type Engine struct {
	IndexPath  string
	IndexStats indexer.IndexStats
	Ranker     *Ranker
	TermPos    map[string]int
	Cache      IndexListCache
	cacheSize  int
	workers    int
}

func NewEngine(srcDir string, cacheSize int, workers int) *Engine {
	indexPath := path.Join(srcDir, "term_list")
	statsPath := path.Join(srcDir, "term_stats")
	posPath := path.Join(srcDir, "term_pos")

	engine := &Engine{
		IndexPath: indexPath,
		TermPos:   map[string]int{},
		cacheSize: cacheSize,
		workers:   workers,
	}

	statsFile, err := os.Open(statsPath)
	if err != nil {
		log.Fatal(err)
	}
	statsDecoder := gob.NewDecoder(statsFile)
	statsDecoder.Decode(&engine.IndexStats)
	engine.Ranker = NewRanker(&engine.IndexStats)

	posFile, err := os.Open(posPath)
	if err != nil {
		log.Fatal(err)
	}
	posDecoder := gob.NewDecoder(posFile)
	posDecoder.Decode(&engine.TermPos)

	diskCache := NewDiskIndexListCache(indexPath, workers, engine.TermPos)
	memCache := NewMemoryIndexListCache(cacheSize, diskCache)
	engine.Cache = memCache

	return engine
}

// empty string if not found
func (ng *Engine) DocURL(docID int) string {
	u, ok := ng.IndexStats.DocIDToURL[docID]
	if !ok {
		return ""
	}
	return u
}

func (ng *Engine) DocURLs(docIDs []int) []string {
	urls := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		urls = append(urls, ng.DocURL(id))
	}
	return urls
}

func (ng *Engine) Process(query string, k int) ([]ResultDoc, error) {
	var result []ResultDoc

	terms := parser.ParseQuery(query)
	log.Println(terms)
	docStats := []*DocStats{}

	for _, term := range terms {
		list, err := ng.Cache.Get(term)
		if err != nil {
			return result, err
		}
		stats := NewDocStats()
		for _, posting := range list.Postings {
			stats.AddTerm(posting.DocID, term)
		}
		docStats = append(docStats, stats)
	}

	mergedStats := MergeDocStats(docStats...)
	scores := ng.Ranker.TFIDF(terms, mergedStats)
	scores = TopK(scores, k)

	result = make([]ResultDoc, 0, len(scores))
	for _, score := range scores {
		result = append(result, ResultDoc{
			DocID:  score.DocID,
			Score:  score.Value,
			DocURL: ng.DocURL(score.DocID),
		})
	}

	return result, nil
}

func (ng *Engine) Run() {

}
