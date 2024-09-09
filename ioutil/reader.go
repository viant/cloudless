package ioutil

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
	"strings"
)

// OpenURL returns uncompressed data reader
func OpenURL(ctx context.Context, fs afs.Service, URL string, options ...storage.Option) (io.ReadCloser, error) {
	reader, err := fs.OpenURL(ctx, URL, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to open: %v, due to %w", URL, err)
	}
	return DataReader(reader, URL)
}

// DataReader returns uncompress data reader
func DataReader(reader io.Reader, URL string) (io.ReadCloser, error) {
	readCloser, ok := reader.(io.ReadCloser)
	if !ok {
		readCloser = ioutil.NopCloser(reader)
	}
	if !strings.HasSuffix(URL, ".gz") {
		return readCloser, nil
	}
	gzReader, err := gzip.NewReader(readCloser)
	if err != nil {
		return nil, err
	}
	return &ReadCloser{ReadCloser: gzReader, Origin: readCloser}, nil
}

// ReadCloser represents a reader closer wrapper of original and wrapper reader
type ReadCloser struct {
	Origin io.ReadCloser
	io.ReadCloser
}

// Stop closes readers
func (c *ReadCloser) Close() error {
	if err := c.Origin.Close(); err != nil {
		return err
	}
	return c.ReadCloser.Close()
}

type bytesSliceReader struct {
	data   [][]byte
	yIndex int
	xIndex int
}

// Read reads data
func (b *bytesSliceReader) Read(out []byte) (n int, err error) {
	if b.yIndex >= len(b.data) {
		return 0, io.EOF
	}
	fragment := b.data[b.yIndex]
	if b.xIndex >= len(fragment) {
		b.xIndex = 0
		b.yIndex++
		return b.Read(out)
	}
	part := fragment[b.xIndex:]
	outLen := len(out)
	partLen := len(part)
	if outLen <= partLen {
		copy(out, part[:outLen])
		b.xIndex += outLen
		return outLen, nil
	}
	copy(out, part)
	b.xIndex += partLen
	return partLen, nil
}

// BytesSliceReader creates a new byte slice reader
func BytesSliceReader(bytes [][]byte) io.Reader {
	return &bytesSliceReader{data: bytes}
}
