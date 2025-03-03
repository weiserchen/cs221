package sys

import (
	"log"
	"os"
	"petersearch/pkg/utils/units"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

func CreateDir(dirname string) error {
	if err := os.RemoveAll(dirname); err != nil {
		return err
	}
	return os.MkdirAll(dirname, 0755)
}

func CreateFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
}

func LogMemoryUsage() {
	const MB = units.MB
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	log.Printf("Memory used: %.2f MB. StackSys: %.2f MB. HeapInUse: %.2f MB.\n",
		float64(memStats.Alloc)/MB, float64(memStats.StackSys)/MB, float64(memStats.HeapInuse)/MB)
}

func LogMemoryProfile() func() error {
	memF, err := os.Create("mem.prof")
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}

	if err := pprof.WriteHeapProfile(memF); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}

	return memF.Close
}

func LogTraceProfile() func() error {
	traceF, err := os.Create("trace.out")
	if err != nil {
		log.Fatal(err)
	}
	err = trace.Start(traceF)
	if err != nil {
		log.Fatal(err)
	}

	return func() error {
		trace.Stop()
		return traceF.Close()
	}
}
