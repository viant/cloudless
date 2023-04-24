package sam

import (
	"bytes"
	"io"
	"net/http"
)

type ProxyResponse struct {
	StatusCode int
	header     http.Header
	bytes.Buffer
}

func (w *ProxyResponse) Header() http.Header {
	return w.header
}

func (w *ProxyResponse) Write(d []byte) (int, error) {
	return w.Buffer.Write(d)
}

func (w *ProxyResponse) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
}

func (w *ProxyResponse) Update(writer http.ResponseWriter) {
	if w.StatusCode != 0 {
		writer.WriteHeader(w.StatusCode)
	}
	for k, vals := range w.header {
		for _, v := range vals {
			writer.Header().Add(k, v)
		}
	}
	if w.Buffer.Len() > 0 {
		io.Copy(writer, &w.Buffer)
	}
}

//NewWriter creates a writer
func NewWriter() *ProxyResponse {
	return &ProxyResponse{header: map[string][]string{}}
}
