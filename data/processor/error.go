package processor

//DataCorruption represents corruption error
type DataCorruption struct {
	message string
}

//Error returns an error
func (e *DataCorruption) Error() string {
	return e.message
}

// NewDataCorruption returns data corruption error
func NewDataCorruption(msg string) error {
	return &DataCorruption{message: msg}
}

func isDataCorruptionError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*DataCorruption)
	return ok
}

//PartialRetry partial retry error allows to write only partial data back to retry stream
type PartialRetry struct {
	data    []byte
	message string
}

//Error returns an error
func (e *PartialRetry) Error() string {
	return e.message
}

// NewDataCorruption returns data corruption error
func NewPartialRetry(msg string, data []byte) error {
	return &PartialRetry{message: msg, data: data}
}

func isPartialRetryError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*PartialRetry)
	return ok
}

type retryError struct {
	message string
}

func (e *retryError) Error() string {
	return e.message
}

// newRetryError returns retry error
func newRetryError(msg string) error {
	return &retryError{message: msg}
}

type processError struct {
	message string
}

func (e *processError) Error() string {
	return e.message
}

// newProcessError returns process error
func newProcessError(msg string) error {
	return &processError{message: msg}
}
