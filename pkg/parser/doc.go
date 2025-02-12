package parser

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type RawDoc struct {
	URL      string `json:"url"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
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

func ParseDir(srcDir string, workerNum int) ([]RawDoc, error) {
	if workerNum == -1 {
		workerNum = runtime.NumCPU() * 2
	}

	rawFiles, err := ReadFiles(srcDir)
	if err != nil {
		return nil, err
	}
	log.Printf("Raw files count: %d\n", len(rawFiles))

	readWorker := func(in <-chan []string, out chan<- []RawDoc, wg *sync.WaitGroup) {
		defer wg.Done()
		for files := range in {
			out <- ReadRawDocs(files)
		}
	}

	rawDocs := []RawDoc{}
	fileCh := make(chan []string)
	docCh := make(chan []RawDoc, workerNum)

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

	for docs := range docCh {
		// fmt.Println("Received")
		rawDocs = append(rawDocs, docs...)
	}
	log.Printf(
		"Parse thread count: %d. Raw docs count: %d. Using %v\n",
		workerNum, len(rawDocs), time.Since(start))

	return rawDocs, nil
}

func readSourceFile() {

}

func removeDupFile() {

}
