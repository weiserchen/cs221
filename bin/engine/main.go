package main

import (
	"fmt"
	"log"
	"petersearch/pkg/engine"
	"time"
)

func main() {
	indexDir := "../../.index"
	cacheSize := 256
	workers := 4
	k := 20

	ng := engine.NewEngine(indexDir, cacheSize, workers)

	rawQueries := []string{
		"Iftekhar ahmed",
		"machine learning",
		"ACM",
		"master of software engineering",
	}

	for _, query := range rawQueries {
		start := time.Now()
		list, err := ng.Process(query, k)
		if err != nil {
			log.Fatal(err)
		}
		for i, item := range list {
			fmt.Printf("%d) %d %s %f\n", i+1, item.DocID, item.DocURL, item.Score)
		}
		fmt.Printf("Total time: %v\n", time.Since(start))
		fmt.Println()
	}
}
