package main

import (
	"log"
	"petersearch/pkg/parser"
)

func main() {
	srcDir := "../../DEV"
	// _, err := parser.ParseDir(srcDir, 4)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	rawFiles, err := parser.ReadFiles(srcDir)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Raw files count: %d\n", len(rawFiles))

	rawDoc, err := parser.ReadRawDoc(rawFiles[0])
	if err != nil {
		log.Fatal(err)
	}

	doc := parser.ParseDoc(rawDoc.Content)
	doc.Print()
}
