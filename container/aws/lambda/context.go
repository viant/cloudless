package lambda

import (
	"github.com/viant/cloudless/container/aws/lambda/invoke"
	"time"
)

type Context struct {
	RequestID          string
	EventBody          string
	FnName             string
	Version            string
	MemSize            string
	Timeout            string
	Region             string
	AccountID          string
	XAmznTraceID       string
	InvokedFunctionArn string
	ClientContext      string
	CognitoIdentity    string
	Start              time.Time
	InvokeWait         time.Time
	InitEnd            time.Time
	TimeoutDuration    time.Duration
	Reply              *invoke.Response
	Done               chan bool
	MaxMem             uint64
	InvocationType     string
	LogType            string
	LogTail            string // base64 encoded tail, no greater than 4096 bytes
	ErrorType          string // Unhandled vs Handled
	Ended              bool
	Ignore             bool
}
