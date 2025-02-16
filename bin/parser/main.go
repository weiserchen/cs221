package main

import (
	"log"
	"petersearch/pkg/parser"
	"petersearch/pkg/utils/stream"
	"sort"
)

func main() {
	srcDir := "../../DEV"
	workers := 1
	consumer := stream.NewArrayConsumer[parser.Doc]()

	rawFiles, err := parser.ReadFiles(srcDir)
	if err != nil {
		log.Fatal(err)
	}

	// For testing
	sort.Strings(rawFiles)
	rawFiles = rawFiles[:1000]

	err = parser.ParseDirDocs(rawFiles, workers, consumer)
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
	doc.Print()
}
