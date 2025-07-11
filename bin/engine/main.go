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

	log.Println("Engine initialization started...")

	ng := engine.NewEngine(indexDir, cacheSize, workers, false)

	log.Println("Engine initialization completed...")

	rawQueries := []string{
		"Iftekhar ahmed",
		"machine learning",
		"ACM",
		"master of software engineering",
	}

	for _, query := range rawQueries {
		start := time.Now()
		list, err := ng.Process(query, k, engine.RankAlgoTFIDF)
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
