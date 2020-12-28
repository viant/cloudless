package processor

import (
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

// Request represents a processing request
type Request struct {
	io.ReadCloser
	Attrs     map[string]interface{}
	StartTime time.Time
	SourceURL string //incoming original filename url
}

//Retry extracts number of retry from URL . It looks after two consecutive digits
// eg: s3://bucket/prefix/filename-retry05.csv would extract number 5
func (r *Request) Retry() int {
	index := strings.LastIndex(r.SourceURL, retryFragment)
	if index == -1 {
		return 0
	}
	retry := r.SourceURL[index+len(retryFragment) : index+len(retryFragment)+2]
	return toolbox.AsInt(retry)

}

// TransformSourceURL returns baseURL + sourceURL path
func (r *Request) TransformSourceURL(baseURL string) string {
	_, pathURL := url.Base(r.SourceURL, file.Scheme)
	return url.Join(baseURL, pathURL)
}

// NewRequest create a processing request
func NewRequest(reader io.Reader, attrs map[string]interface{}, sourceURL string) *Request {
	readCloser, ok := reader.(io.ReadCloser)
	if !ok {
		readCloser = ioutil.NopCloser(reader)
	}
	return &Request{
		ReadCloser: readCloser,
		Attrs:      attrs,
		StartTime:  time.Now(),
		SourceURL:  sourceURL,
	}
}
