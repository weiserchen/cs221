package engine

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"petersearch/pkg/indexer"
	"petersearch/pkg/parser"
	"strings"
	"sync"
	"time"

	"github.com/c-bata/go-prompt"
)

type ResultDoc struct {
	DocID  uint64
	DocURL string
	Score  float64
}

type Engine struct {
	IndexPath  string
	IndexStats indexer.IndexStats
	PosStats   indexer.PosStats
	Ranker     *Ranker
	ListCache  IndexListCache
	TermCache  *GeneralCache[*DocStats]
	QueryCache *GeneralCache[[]Score]
	cacheSize  int
	workers    int
}

func NewEngine(srcDir string, cacheSize int, workers int, compress bool) *Engine {
	indexPath := path.Join(srcDir, "term_list")
	statsPath := path.Join(srcDir, "term_stats")
	posPath := path.Join(srcDir, "term_pos")

	engine := &Engine{
		IndexPath: indexPath,
		cacheSize: cacheSize,
		workers:   workers,
	}

	statsFile, err := os.Open(statsPath)
	if err != nil {
		log.Fatal(err)
	}
	statsDecoder := gob.NewDecoder(statsFile)
	statsDecoder.Decode(&engine.IndexStats)
	log.Println("Stats file decoded...")
	engine.Ranker = NewRanker(&engine.IndexStats)

	posFile, err := os.Open(posPath)
	if err != nil {
		log.Fatal(err)
	}
	engine.PosStats = indexer.ReadPosFile(posFile)
	log.Println("Pos File decoded...")

	diskListCache := NewDiskIndexListCache(indexPath, workers, engine.PosStats, compress)
	log.Println("Disk cache initialized...")

	// memListCache := NewMemoryIndexListCache(cacheSize, diskListCache)
	// log.Println("Mem cache initialized...")

	engine.ListCache = diskListCache
	engine.TermCache = NewGeneralCache[*DocStats](cacheSize * 2)
	engine.QueryCache = NewGeneralCache[[]Score](cacheSize)

	return engine
}

// empty string if not found
func (ng *Engine) DocURL(docID uint64) string {
	u, ok := ng.IndexStats.DocIDToURL[docID]
	if !ok {
		return ""
	}
	return u
}

func (ng *Engine) DocURLs(docIDs []uint64) []string {
	urls := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		urls = append(urls, ng.DocURL(id))
	}
	return urls
}

func (ng *Engine) Process(query string, k int, ranker RankAlgo) ([]ResultDoc, error) {
	var result []ResultDoc
	var mergedStats *DocStats

	terms := parser.ParseQuery(query)
	fmt.Printf("Expanded Terms: %v\n", terms)

	var scores []Score
	var err error
	normalizedQuery := strings.Join(terms, " ")
	scores, err = ng.QueryCache.Get(normalizedQuery)
	if err != nil {
		docStats := []*DocStats{}
		statsCh := make(chan *DocStats)

		var wg sync.WaitGroup
		wg.Add(len(terms))
		for _, term := range terms {
			go func() {
				defer wg.Done()
				var stats *DocStats
				var err error

				stats, err = ng.TermCache.Get(term)
				if err != nil {
					list, err := ng.ListCache.Get(term)
					if err != nil {
						log.Println(err)
						return
					}

					stats = NewDocStats()
					for _, posting := range list.Postings {
						stats.AddPosting(term, posting)
					}
					ng.TermCache.Set(term, stats)
				}

				statsCh <- stats
			}()
		}

		go func() {
			defer close(statsCh)
			wg.Wait()
		}()

		for stats := range statsCh {
			docStats = append(docStats, stats)
		}

		mergedStats = MergeDocStats(docStats...)
		switch ranker {
		case RankAlgoTFIDF:
			scores = ng.Ranker.TFIDF(terms, mergedStats)
		default:
			scores = ng.Ranker.BM25(terms, mergedStats)
		}
		ng.QueryCache.Set(normalizedQuery, scores)
	}

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

func (ng *Engine) RunCLI(k int, ranker RankAlgo) {
	defer restoreTerminal()
	p := prompt.New(
		cliExecutor(ng, k, ranker),
		cliCompleter,
		prompt.OptionPrefix("Enter Query: "),
		prompt.OptionTitle("cli-interface"),
		// prompt.OptionAddKeyBind(prompt.KeyBind{
		// 	Key: prompt.ControlD,
		// 	Fn: func(b *prompt.Buffer) {
		// 		println("Exiting go-prompt...")
		// 		restoreTerminal()
		// 	},
		// }),
	)
	p.Run()
}

// Restore terminal settings before exiting
func restoreTerminal() {
	rawModeOff := exec.Command("/bin/stty", "-raw", "sane")
	rawModeOff.Stdin = os.Stdin
	_ = rawModeOff.Run()
	rawModeOff.Wait()

}

func cliExecutor(ng *Engine, k int, ranker RankAlgo) func(string) {
	return func(query string) {
		scoreQuery(ng, k, ranker, query)
	}
}

func scoreQuery(ng *Engine, k int, ranker RankAlgo, query string) {
	start := time.Now()
	list, err := ng.Process(query, k, ranker)
	if err != nil {
		if err == ErrCacheEntryNotFound {
			fmt.Println("No documents!")
		} else {
			log.Fatal(err)
		}
	}

	var sb strings.Builder
	for i, item := range list {
		fmt.Fprintf(&sb, "%d) %d %s %f\n", i+1, item.DocID, item.DocURL, item.Score)
	}
	totalEnd := time.Since(start)
	fmt.Fprintf(&sb, "Total Time: %v\n", totalEnd)
	fmt.Fprintf(&sb, "Type ctrl+d to exit.\n")
	fmt.Println(sb.String())
}

func cliCompleter(in prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}
	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}
