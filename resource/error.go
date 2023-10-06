package resource

import (
	"strings"
	"sync"
)

type Error struct {
	mux    sync.Mutex
	Errors []error
}

func (e *Error) Error() string {
	e.mux.Lock()
	defer e.mux.Unlock()
	if len(e.Errors) == 0 {
		return ""
	}
	builder := strings.Builder{}
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	for i, err := range e.Errors {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(err.Error())
	}
	return builder.String()
}

func (e *Error) HasError() bool {
	e.mux.Lock()
	defer e.mux.Unlock()
	ret := len(e.Errors) > 0
	return ret
}

func (e *Error) Append(err error) {
	if err == nil {
		return
	}
	e.mux.Lock()
	defer e.mux.Unlock()
	e.Errors = append(e.Errors, err)
}
