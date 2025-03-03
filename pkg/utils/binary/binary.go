package binary

import (
	"bufio"
	"encoding/binary"
	"io"
)

type BufferedWriteCloser struct {
	w     *bufio.Writer
	wc    io.WriteCloser
	count int
}

func NewBufferedWriteCloser(w io.WriteCloser) *BufferedWriteCloser {
	return &BufferedWriteCloser{
		w:  bufio.NewWriter(w),
		wc: w,
	}
}

func (bw *BufferedWriteCloser) Total() int {
	return bw.count
}

func (bw *BufferedWriteCloser) Buffered() int {
	return bw.w.Buffered()
}

func (bw *BufferedWriteCloser) Write(p []byte) (n int, err error) {
	bw.count += len(p)
	return bw.w.Write(p)
}

func (bw *BufferedWriteCloser) Close() error {
	if err := bw.w.Flush(); err != nil {
		return err
	}
	bw.w = nil
	return bw.wc.Close()
}

type BufferedReadCloser struct {
	r  *bufio.Reader
	rc io.ReadCloser
}

func NewBufferedReadCloser(r io.ReadCloser) *BufferedReadCloser {
	return &BufferedReadCloser{
		r:  bufio.NewReader(r),
		rc: r,
	}
}

func (br *BufferedReadCloser) Read(p []byte) (n int, err error) {
	return io.ReadFull(br.r, p)
}

func (br *BufferedReadCloser) Close() error {
	br.r = nil
	return br.rc.Close()
}

type ByteWriter struct {
	w io.WriteCloser
}

func NewByteWriter(w io.WriteCloser) *ByteWriter {
	return &ByteWriter{
		w: w,
	}
}

func NewBufferedByteWriter(w io.WriteCloser) *ByteWriter {
	return NewByteWriter(NewBufferedWriteCloser(w))
}

func (bw *ByteWriter) WriteBytes(b []byte) error {
	var err error
	err = binary.Write(bw.w, binary.LittleEndian, int64(len(b)))
	if err != nil {
		return err
	}
	_, err = bw.w.Write(b)
	return err
}

func (bw *ByteWriter) WriteString(s string) error {
	return bw.WriteBytes([]byte(s))
}

func (bw *ByteWriter) WriteInt(i int) error {
	return binary.Write(bw.w, binary.LittleEndian, int64(i))
}

func (bw *ByteWriter) Close() error {
	return bw.w.Close()
}

type ByteReader struct {
	r io.ReadCloser
}

func NewByteReader(r io.ReadCloser) *ByteReader {
	return &ByteReader{
		r: r,
	}
}

func NewBufferedByteReader(r io.ReadCloser) *ByteReader {
	return NewByteReader(NewBufferedReadCloser(r))
}

func (br *ByteReader) ReadBytes() ([]byte, error) {
	var err error
	var length int64
	err = binary.Read(br.r, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}
	b := make([]byte, length)
	_, err = br.r.Read(b)
	return b, err
}

func (br *ByteReader) ReadString() (string, error) {
	b, err := br.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (br *ByteReader) ReadInt() (int, error) {
	var i int64
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return int(i), err
}

func (br *ByteReader) Close() error {
	return br.r.Close()
}
