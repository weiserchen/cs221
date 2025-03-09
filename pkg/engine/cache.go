package engine

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
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

type GeneralCache[T any] struct {
	cache *lru.Cache[string, T]
}

func NewGeneralCache[T any](size int) *GeneralCache[T] {
	cache, _ := lru.New[string, T](size)
	return &GeneralCache[T]{
		cache: cache,
	}
}

func (gc *GeneralCache[T]) Get(term string) (T, error) {
	var v T
	v, ok := gc.cache.Get(term)
	if !ok {
		return v, ErrCacheEntryNotFound
	}
	return v, nil
}

func (gc *GeneralCache[T]) Set(term string, v T) error {
	gc.cache.Add(term, v)
	return nil
}

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
	posStats  indexer.PosStats
	compress  bool
}

type termRequest struct {
	term     string
	posStart uint64
	posEnd   uint64
	resultCh chan<- termResponse
}

type termResponse struct {
	result indexer.InvertedList
	err    error
}

func NewDiskIndexListCache(filename string, workers int, posStats indexer.PosStats, compress bool) *DiskIndexListCache {
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
				req.resultCh <- getTermFromFile(f, req.term, req.posStart, req.posEnd, compress)
			}
		}()
	}

	return &DiskIndexListCache{
		accessCh:  accessCh,
		accessors: accessors,
		workers:   workers,
		posStats:  posStats,
		compress:  compress,
	}
}

func (dc *DiskIndexListCache) Get(term string) (indexer.InvertedList, error) {
	posStart, ok := dc.posStats.TermStart[term]
	if !ok {
		return indexer.InvertedList{}, ErrCacheEntryNotFound
	}
	posEnd, ok := dc.posStats.TermEnd[term]
	if !ok {
		return indexer.InvertedList{}, ErrCacheEntryNotFound
	}

	resultCh := make(chan termResponse)
	req := termRequest{
		term:     term,
		posStart: posStart,
		posEnd:   posEnd,
		resultCh: resultCh,
	}
	dc.accessCh <- req

	resp := <-resultCh
	return resp.result, resp.err
}

func (dc *DiskIndexListCache) Set(term string, list indexer.InvertedList) error {
	return ErrCacheSetOpertationNotSupported
}

func getTermFromFile(f *os.File, term string, posStart, posEnd uint64, compress bool) termResponse {
	var list indexer.InvertedList

	if _, err := f.Seek(int64(posStart), 0); err != nil {
		return termResponse{
			err: err,
		}
	}

	buf := make([]byte, posEnd-posStart)
	_, err := io.ReadFull(f, buf)
	if err != nil {
		return termResponse{
			err: err,
		}
	}

	var r io.ReadCloser
	if compress {
		gzReader, err := gzip.NewReader(bytes.NewReader(buf))
		if err != nil {
			return termResponse{
				err: err,
			}
		}
		r = gzReader
	} else {
		r = io.NopCloser(bytes.NewReader(buf))
	}

	br := binary.NewBufferedByteReader(r)
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
