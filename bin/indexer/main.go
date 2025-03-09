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

	batchSize := 10_000_000
	batchCount := 1000
	tasks := -1
	workers := 4
	srcDir := "../../DEV"
	dstDir := "../../.index"
	compress := false

	indexer.BuildIndex(batchSize, batchCount, tasks, workers, srcDir, dstDir, compress)
}
