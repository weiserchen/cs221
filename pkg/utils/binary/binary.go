package binary

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

type BufferedWriteCloser struct {
	w     *bufio.Writer
	wc    io.WriteCloser
	count uint64
}

func NewBufferedWriteCloser(w io.WriteCloser) *BufferedWriteCloser {
	return &BufferedWriteCloser{
		w:  bufio.NewWriter(w),
		wc: w,
	}
}

func (bw *BufferedWriteCloser) Total() uint64 {
	return bw.count
}

func (bw *BufferedWriteCloser) Buffered() int {
	return bw.w.Buffered()
}

func (bw *BufferedWriteCloser) Write(p []byte) (n int, err error) {
	bw.count += uint64(len(p))
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

type MemBuf struct {
	buf bytes.Buffer
}

func NewMemBuf() *MemBuf {
	return &MemBuf{}
}

func (mb *MemBuf) Read(p []byte) (n int, err error) {
	return mb.buf.Read(p)
}

func (mb *MemBuf) Write(p []byte) (n int, err error) {
	return mb.buf.Write(p)
}

func (mb *MemBuf) Bytes() []byte {
	return mb.buf.Bytes()
}

func (mb *MemBuf) Close() error {
	return nil
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

func (bw *ByteWriter) Write(b []byte) (int, error) {
	return bw.w.Write(b)
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

func (bw *ByteWriter) WriteUInt64(i uint64) error {
	return binary.Write(bw.w, binary.LittleEndian, i)
}

func (bw *ByteWriter) WriteUInt32(i uint32) error {
	return binary.Write(bw.w, binary.LittleEndian, i)
}

func (bw *ByteWriter) WriteUInt16(i uint16) error {
	return binary.Write(bw.w, binary.LittleEndian, i)
}

func (bw *ByteWriter) WriteUInt8(i uint8) error {
	return binary.Write(bw.w, binary.LittleEndian, i)
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

func (br *ByteReader) ReadByte() (byte, error) {
	var b byte
	err := binary.Read(br.r, binary.LittleEndian, &b)
	return b, err
}

func (br *ByteReader) ReadInt() (int, error) {
	var i int64
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return int(i), err
}

func (br *ByteReader) ReadUInt64() (uint64, error) {
	var i uint64
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return i, err
}

func (br *ByteReader) ReadUInt32() (uint32, error) {
	var i uint32
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return i, err
}

func (br *ByteReader) ReadUInt16() (uint16, error) {
	var i uint16
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return i, err
}

func (br *ByteReader) ReadUInt8() (uint8, error) {
	var i uint8
	err := binary.Read(br.r, binary.LittleEndian, &i)
	return i, err
}

func (br *ByteReader) ReadHeader() (uint8, uint8, error) {
	header, err := br.ReadUInt8()
	if err != nil {
		return 0, 0, err
	}
	tagType := (header & 12) >> 2
	docIDLen := header & 3
	return tagType, docIDLen, nil
}

func (br *ByteReader) Close() error {
	return br.r.Close()
}
