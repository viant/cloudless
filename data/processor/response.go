package processor

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//StatusSet
type StatusSet int

const (
	StatusOk           = "ok"
	StatusError        = "error"
	StatusSetOk        = StatusSet(1)
	StatusSetError     = StatusSet(2)
	StatusSetRetriable = StatusSet(4)
	StatusSetCorrupted = StatusSet(8)
)

func (s StatusSet) String() string {
	builder := strings.Builder{}
	if s&StatusSetOk == StatusSetOk {
		builder.WriteString(StatusOk)
	}
	if s&StatusSetRetriable == StatusSetRetriable {
		if builder.Len() > 0 {
			builder.WriteString("|")
		}
		builder.WriteString("retry")
	}
	if s&StatusSetCorrupted == StatusSetCorrupted {
		if builder.Len() > 0 {
			builder.WriteString("|")
		}
		builder.WriteString("corrupted")
	}
	if s&StatusSetError == StatusSetError {
		return StatusError
	}
	return builder.String()
}

// Response represents base processing response
type Response struct {
	Status           string
	statusSet        StatusSet
	Errors           []string `json:",omitempty"`
	mutex            sync.Mutex
	StartTime        time.Time
	RuntimeMs        int
	SourceURL        string `json:",omitempty"`
	DestinationURL   string `json:",omitempty"` // Service processing data destination URL. This is a template, e.g. $gs://$mybucket/$prefix/$a.dat
	DestinationCodec string `json:"-"`          //optional compression codec (i.e gzip)
	RetryURL         string `json:"-"`          // destination for the data to be replayed
	CorruptionURL    string `json:"-"`
	Processed        int32  `json:",omitempty"`
	RetryErrors      int32  `json:",omitempty"`
	CorruptionErrors int32  `json:",omitempty"`
	RetriableErrors  int32  `json:",omitempty"`
	Loaded           int32  `json:",omitempty"`
	LoadTimeouts     int32  `json:",omitempty"`
	Batched          int32  `json:",omitempty"`
	Skipped          int32  `json:",omitempty"`
}

// LogError logs error
func (r *Response) LogError(err error) {
	if err == nil {
		return
	}

	var counter *int32
	switch err.(type) {
	case *retryError:
		counter = &r.RetryErrors
		r.statusSet |= StatusSetError
	case *processError:
		counter = &r.RetriableErrors
		r.statusSet |= StatusSetRetriable
	case *DataCorruption:
		counter = &r.CorruptionErrors
		r.statusSet |= StatusSetCorrupted
	default:
		r.statusSet |= StatusSetError
	}
	if counter != nil { //store only one error of the error above
		if atomic.AddInt32(counter, 1) > 1 {
			return
		}
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.Status = r.statusSet.String()
	if len(r.Errors) == 0 {
		r.Errors = make([]string, 0)
	}
	errMsg := err.Error()
	if len(errMsg) > 256 {
		errMsg = errMsg[:256] + "..."
	}
	r.Errors = append(r.Errors, errMsg)
}
