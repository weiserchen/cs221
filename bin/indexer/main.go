package main

import (
	"petersearch/pkg/indexer"
	"petersearch/pkg/utils/sys"
	"time"
)

func main() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			sys.LogMemoryUsage()
		}
	}()

	batch := 100
	// tasks := 100
	tasks := -1
	workers := 2
	srcDir := "../../DEV"
	dstDir := "../../.index"
	compress := false

	indexer.BuildIndex(batch, tasks, workers, srcDir, dstDir, compress)
}
