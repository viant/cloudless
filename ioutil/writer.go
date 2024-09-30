package ioutil

import "io"

// WriterCloser represents a writer closer wrapper of original and wrapper
type WriterCloser struct {
	Origin io.WriteCloser
	io.WriteCloser
}

// Stop closes writers
func (w WriterCloser) Close() error {
	if flusher, ok := w.WriteCloser.(Flusher); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}
	if err := w.WriteCloser.Close(); err != nil {
		return err
	}
	return w.Origin.Close()
}
