package engine

import (
	"errors"
	"log"
	"os"
	"petersearch/pkg/indexer"
	"petersearch/pkg/utils/binary"

	lru "github.com/hashicorp/golang-lru/v2"
)

var (
	ErrCacheEntryNotFound             = errors.New("cache entry not found")
	ErrCacheSetOpertationNotSupported = errors.New("cache set operation is not supported")
)

type IndexListCache interface {
	Get(term string) (indexer.InvertedList, error)
	Set(term string, list indexer.InvertedList) error
}

var _ IndexListCache = (*MemoryIndexListCache)(nil)
var _ IndexListCache = (*DiskIndexListCache)(nil)

type MemoryIndexListCache struct {
	cache *lru.Cache[string, indexer.InvertedList]
	src   IndexListCache
}

func NewMemoryIndexListCache(size int, src IndexListCache) *MemoryIndexListCache {
	cache, _ := lru.New[string, indexer.InvertedList](size)
	return &MemoryIndexListCache{
		cache: cache,
		src:   src,
	}
}

func (mc *MemoryIndexListCache) Get(term string) (indexer.InvertedList, error) {
	var list indexer.InvertedList

	list, ok := mc.cache.Get(term)
	if !ok {
		if mc.src == nil {
			return list, ErrCacheEntryNotFound
		}
		list, err := mc.src.Get(term)
		if err != nil {
			return list, err
		}
		mc.Set(term, list)
		return list, nil
	}

	return list, nil
}

func (mc *MemoryIndexListCache) Set(term string, list indexer.InvertedList) error {
	_ = mc.cache.Add(term, list)
	return nil
}

type DiskIndexListCache struct {
	accessCh  chan termRequest
	accessors []*os.File
	workers   int
	termPos   map[string]int
}

type termRequest struct {
	term     string
	pos      int
	resultCh chan<- termResponse
}

type termResponse struct {
	result indexer.InvertedList
	err    error
}

func NewDiskIndexListCache(filename string, workers int, termPos map[string]int) *DiskIndexListCache {
	accessCh := make(chan termRequest, workers)
	var accessors []*os.File
	for range workers {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalf("failed to open file in disk cache: %v\n", err)
		}
		accessors = append(accessors, f)
		go func() {
			defer func() {
				f.Close()
			}()
			for req := range accessCh {
				req.resultCh <- getTermFromFile(f, req.term, req.pos)
			}
		}()
	}

	return &DiskIndexListCache{
		accessCh:  accessCh,
		accessors: accessors,
		workers:   workers,
		termPos:   termPos,
	}
}

func (dc *DiskIndexListCache) Get(term string) (indexer.InvertedList, error) {
	pos, ok := dc.termPos[term]
	if !ok {
		return indexer.InvertedList{}, ErrCacheEntryNotFound
	}

	resultCh := make(chan termResponse)
	req := termRequest{
		term:     term,
		pos:      pos,
		resultCh: resultCh,
	}
	dc.accessCh <- req

	resp := <-resultCh
	return resp.result, resp.err
}

func (dc *DiskIndexListCache) Set(term string, list indexer.InvertedList) error {
	return ErrCacheSetOpertationNotSupported
}

func getTermFromFile(f *os.File, term string, pos int) termResponse {
	var list indexer.InvertedList

	if _, err := f.Seek(int64(pos), 0); err != nil {
		return termResponse{
			err: err,
		}
	}

	br := binary.NewBufferedByteReader(f)
	listIter, err := indexer.ReadInvertedList(br)
	if err != nil {
		return termResponse{
			err: err,
		}
	}

	list.Term = term
	list.Postings = indexer.CollectInvertedList(listIter)
	return termResponse{
		result: list,
		err:    nil,
	}
}
