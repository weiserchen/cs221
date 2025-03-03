package engine

import (
	"encoding/gob"
	"log"
	"os"
	"path"
	"petersearch/pkg/indexer"
)

type Engine struct {
	IndexPath  string
	IndexStats indexer.IndexStats
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
func (eg *Engine) DocURL(docID int) string {
	u, ok := eg.IndexStats.DocIDToURL[docID]
	if !ok {
		return ""
	}
	return u
}

func (eg *Engine) DocURLs(docIDs []int) []string {
	urls := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		urls = append(urls, eg.DocURL(id))
	}
	return urls
}
