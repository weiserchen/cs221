package main

import (
	"log"
	"petersearch/pkg/engine"
)

func main() {
	indexDir := "../../.index"
	cacheSize := 256
	workers := 20
	k := 20

	log.Println("Engine initialization started...")

	ng := engine.NewEngine(indexDir, cacheSize, workers, false)

	log.Println("Engine initialization completed...")

	ng.Run(k, engine.RankAlgoBM25)
}
