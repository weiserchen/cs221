package parser

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"petersearch/pkg/utils/stream"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfonda/simhash"
)

var stopwords map[string]struct{}

//go:embed stopwords.txt
var stopwordsStr string

func init() {
	stopwords = map[string]struct{}{}

	words := strings.Fields(stopwordsStr)
	for _, word := range words {
		stopwords[word] = struct{}{}
	}
}

type RawDoc struct {
	URL      string `json:"url"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
	size     int
}

type Doc struct {
	ID         uint64
	URL        string
	Tokens     []string
	TwoGrams   []string
	ThreeGrams []string
	TagMap     map[string][]string
	RawSize    int
}

func (doc *Doc) Print() {
	fmt.Println(doc.Tokens)
	for tag, list := range doc.TagMap {
		fmt.Printf("%s: %v\n", tag, list)
	}
}

func ReadFiles(srcDir string) ([]string, error) {
	validFiles := []string{}

	dirs, err := os.ReadDir(srcDir)
	if err != nil {
		return nil, err
	}

	for _, dirName := range dirs {
		files, err := os.ReadDir(filepath.Join(srcDir, dirName.Name()))
		if err != nil {
			return nil, err
		}

		for _, file := range files {
			validFiles = append(validFiles, filepath.Join(srcDir, dirName.Name(), file.Name()))
		}
	}

	log.Printf("Raw files count: %d\n", len(validFiles))

	return validFiles, nil
}

func ReadRawDocs(files []*os.File) []RawDoc {
	var rawDocs []RawDoc
	for _, file := range files {
		rawDoc, err := ReadRawDoc(file)
		if err != nil {
			log.Fatal(err)
		}
		rawDocs = append(rawDocs, rawDoc)
	}
	return rawDocs
}

func ReadRawDoc(f *os.File) (RawDoc, error) {
	var rawDoc RawDoc

	// fmt.Println("Read raw")
	b, err := io.ReadAll(f)
	if err != nil {
		log.Printf("failed to read file: %s\n", f.Name())
		return rawDoc, err
	}

	err = json.Unmarshal(b, &rawDoc)
	if err != nil {
		log.Printf("failed to unmarshal file\n")
		return rawDoc, err
	}
	rawDoc.size = len(b)

	return rawDoc, nil
}

func ParseDirDocs(rawFiles []string, workerNum, batchSize, batchCount int, consumer stream.Consumer) error {
	if workerNum <= 0 {
		workerNum = runtime.NumCPU() * 2
	}

	log.Printf("Batch Size: %d MB, Batch Count: %d\n", batchSize/1000_000, batchCount)

	largeFileCount := atomic.Int64{}
	smallFileCount := atomic.Int64{}
	dupFileCount := atomic.Int64{}

	dupMap := sync.Map{}
	readWorker := func(in <-chan string, out chan<- []Doc, wg *sync.WaitGroup) {
		defer wg.Done()

		files := []*os.File{}
		currSize := 0
		currCount := 0
		processFunc := func(files []*os.File) {
			for _, file := range files {
				defer file.Close()
			}

			rawDocs := ReadRawDocs(files)
			docs := ParseDocs(rawDocs)
			newDocs := []Doc{}
			for _, doc := range docs {
				if len(doc.Tokens) < 100 {
					smallFileCount.Add(1)
					continue
				}
				if len(doc.Tokens) > 65535 {
					largeFileCount.Add(1)
					continue
				}
				hash := simhash.Simhash(simhash.NewWordFeatureSet([]byte(strings.Join(doc.Tokens, " "))))
				if _, ok := dupMap.LoadOrStore(hash, struct{}{}); ok {
					dupFileCount.Add(1)
				} else {
					newDocs = append(newDocs, doc)
				}
			}
			out <- newDocs
		}

		for filename := range in {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			fstats, err := file.Stat()
			if err != nil {
				log.Fatal(err)
			}

			// ignore small and large files
			fileSize := int(fstats.Size())
			if fileSize <= 100 {
				smallFileCount.Add(1)
				file.Close()
				continue
			}
			if fileSize >= batchSize*2 {
				largeFileCount.Add(1)
				file.Close()
				continue
			}

			currSize += fileSize
			currCount++
			files = append(files, file)
			if currSize < batchSize && currCount < batchCount {
				continue
			}
			log.Printf("Processing Batch: (Size: %d MB, Count: %d)\n", currSize/1000_000, currCount)

			processFunc(files)
			currCount = 0
			currSize = 0
			files = []*os.File{}
		}

		if len(files) > 0 {
			processFunc(files)
		}
	}

	fileCh := make(chan string)
	docCh := make(chan []Doc)

	var wg sync.WaitGroup
	wg.Add(workerNum)
	for range workerNum {
		go readWorker(fileCh, docCh, &wg)
	}

	start := time.Now()

	go func() {
		defer close(docCh)
		wg.Wait()
		log.Println("Parse phase completed.")
	}()

	go func() {
		defer close(fileCh)
		for _, file := range rawFiles {
			fileCh <- file
		}
	}()

	var docID uint64
	for docs := range docCh {
		for _, doc := range docs {
			doc.ID = docID
			docID++
			consumer.Consume(doc)
		}
	}
	log.Printf(
		"Parse thread count: %d. Processed Raw Docs: %d. Large Files: %d. Small Files: %d. Duplicate Files: %d. Using %v\n",
		workerNum, docID, largeFileCount.Load(), smallFileCount.Load(), dupFileCount.Load(), time.Since(start))

	return nil
}

func ParseDocs(rawDocs []RawDoc) []Doc {
	docs := []Doc{}
	for _, rawDoc := range rawDocs {
		doc := ParseDoc(rawDoc)
		docs = append(docs, doc)
	}
	return docs
}

func ParseDoc(rawDoc RawDoc) Doc {
	content := Sanitize(rawDoc.Content)
	tokens := ParseTokens(content)
	tokens = StemTokens(tokens)
	twoGrams := TwoGrams(tokens)
	threeGrams := ThreeGrams(tokens)
	rawTagMap := ExtractTagMap(rawDoc.Content)
	tagMap := ParseTagMap(rawTagMap)
	return Doc{
		URL:        rawDoc.URL,
		Tokens:     tokens,
		TwoGrams:   twoGrams,
		ThreeGrams: threeGrams,
		TagMap:     tagMap,
		RawSize:    rawDoc.size,
	}
}

func ParseQuery(query string) []string {
	query = Sanitize(query)
	tokens := ParseTokens(query)
	tokens = StemTokens(tokens)
	twoGrams := TwoGrams(tokens)
	threeGrams := ThreeGrams(tokens)
	tokens = append(tokens, twoGrams...)
	tokens = append(tokens, threeGrams...)

	validTokens := []string{}
	for _, token := range tokens {
		if _, ok := stopwords[token]; !ok {
			validTokens = append(validTokens, token)
		}
	}
	return validTokens
}
