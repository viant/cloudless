package processor

import (
	"compress/gzip"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/cloudless/ioutil"
	"io"
	"strings"
	"sync"
)

//Writer represents text data writer
type Writer struct {
	writer  io.WriteCloser
	mutex   sync.Mutex
	codec   string
	counter int32
	url     string
	fs      afs.Service
}

func (w *Writer) Write(ctx context.Context, data []byte) (err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.counter == 0 {
		writer, err := w.fs.NewWriter(ctx, w.url, file.DefaultFileOsMode)
		if err != nil {
			return err
		}
		if w.codec == "gzip" {
			w.writer = &ioutil.WriterCloser{WriteCloser: gzip.NewWriter(writer), Origin: writer}
		} else {
			w.writer = writer
		}
	} else {
		_, err = w.writer.Write([]byte{'\n'})
		if err != nil {
			return err
		}
	}
	_, err = w.writer.Write(data)
	w.counter++
	return err
}

//Close closes the writer if there are any writes
func (w *Writer) Close() error {
	if w.counter == 0 {
		return nil
	}
	return w.writer.Close()
}

//NewWriter creates a writer
func NewWriter(URL string, fs afs.Service) *Writer {
	codec := ""
	if strings.HasSuffix(URL, ".gz") {
		codec = "gzip"
	}
	return &Writer{url: URL, fs: fs, codec: codec}
}
