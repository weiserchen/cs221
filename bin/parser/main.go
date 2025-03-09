package main

import (
	"fmt"
	"log"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"sort"
)

func main() {
	srcDir := "../../DEV"
	workers := 1
	batchSize := 100_000_000 // 100MB
	batchCount := 100
	consumer := stream.NewArrayConsumer[parser.Doc]()

	rawFiles, err := parser.ReadFiles(srcDir)
	if err != nil {
		log.Fatal(err)
	}

	// For testing
	sort.Strings(rawFiles)
	rawFiles = rawFiles[:1000]

	err = parser.ParseDirDocs(rawFiles, workers, batchSize, batchCount, consumer)
	if err != nil {
		log.Fatal(err)
	}

	// rawFiles, err := parser.ReadFiles(srcDir)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("Raw files count: %d\n", len(rawFiles))

	// rawDoc, err := parser.ReadRawDoc(rawFiles[0])
	// if err != nil {
	// 	log.Fatal(err)
	// }
	doc := consumer.Collect()[0]
	fmt.Println(len(doc.Tokens))
	doc.Print()
}
