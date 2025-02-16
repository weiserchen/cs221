package parser

import (
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
	"time"

	"github.com/mfonda/simhash"
)

type RawDoc struct {
	URL      string `json:"url"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

type Doc struct {
	ID     int
	URL    string
	Tokens []string
	TagMap map[string][]string
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

func ReadRawDocs(files []string) []RawDoc {
	var rawDocs []RawDoc
	for _, file := range files {
		rawDoc, err := ReadRawDoc(file)
		if err != nil {
			continue
		}
		rawDocs = append(rawDocs, rawDoc)
	}
	return rawDocs
}

func ReadRawDoc(file string) (RawDoc, error) {
	var rawDoc RawDoc

	f, err := os.Open(file)
	if err != nil {
		log.Printf("failed to open file: %s\n", file)
		return rawDoc, err
	}

	b, err := io.ReadAll(f)
	if err != nil {
		log.Printf("failed to read file: %s\n", file)
		return rawDoc, err
	}

	err = json.Unmarshal(b, &rawDoc)
	if err != nil {
		log.Printf("failed to unmarshal file\n")
		return rawDoc, err
	}

	return rawDoc, nil
}

func ParseDirDocs(rawFiles []string, workerNum int, consumer stream.Consumer) error {
	if workerNum <= 0 {
		workerNum = runtime.NumCPU() * 2
	}

	dupMap := sync.Map{}
	// binmask := ^uint64(7)
	readWorker := func(in <-chan []string, out chan<- []Doc, wg *sync.WaitGroup) {
		defer wg.Done()
		for files := range in {
			rawDocs := ReadRawDocs(files)
			docs := ParseDocs(rawDocs)
			newDocs := []Doc{}
			for _, doc := range docs {
				// hash := simhash.Simhash(simhash.NewWordFeatureSet([]byte(strings.Join(doc.Tokens, " ")))) & binmask
				hash := simhash.Simhash(simhash.NewWordFeatureSet([]byte(strings.Join(doc.Tokens, " "))))
				if _, ok := dupMap.LoadOrStore(hash, struct{}{}); !ok {
					newDocs = append(newDocs, doc)
				}
			}
			out <- newDocs
		}
	}

	docs := []Doc{}
	fileCh := make(chan []string)
	docCh := make(chan []Doc, workerNum)

	var wg sync.WaitGroup
	wg.Add(workerNum)
	for range workerNum {
		go readWorker(fileCh, docCh, &wg)
	}

	start := time.Now()
	batch := 100
	go func() {
		defer close(fileCh)
		for {
			if len(rawFiles) >= batch {
				files := rawFiles[:batch]
				rawFiles = rawFiles[batch:]
				fileCh <- files
			} else {
				fileCh <- rawFiles
				break
			}
		}
	}()

	go func() {
		defer close(docCh)
		wg.Wait()
		log.Println("Parse phase completed.")
	}()

	docID := 0
	for docs := range docCh {
		// fmt.Println("Received")
		for _, doc := range docs {
			doc.ID = docID
			docID++
			consumer.Consume(doc)
		}
	}
	log.Printf(
		"Parse thread count: %d. Raw docs count: %d. Using %v\n",
		workerNum, len(docs), time.Since(start))

	return nil
}

func ParseDocs(rawDocs []RawDoc) []Doc {
	docs := []Doc{}
	for _, rawDoc := range rawDocs {
		docs = append(docs, ParseDoc(rawDoc))
	}
	return docs
}

func ParseDoc(rawDoc RawDoc) Doc {
	content := Sanitize(rawDoc.Content)
	tokens := ParseTokens(content)
	rawTagMap := ExtractTagMap(rawDoc.Content)
	tagMap := ParseTagMap(rawTagMap)
	return Doc{
		URL:    rawDoc.URL,
		Tokens: tokens,
		TagMap: tagMap,
	}
}
