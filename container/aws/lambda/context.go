package lambda

import (
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Context struct {
	RequestID          string
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
	InvokePayload      []byte
	InvokeResponse     []byte
	InvokeError        string
	MaxMem             uint64
	LogType            string
}

func (c *Context) ParseTimeout() {
	timeoutDuration, err := time.ParseDuration(c.Timeout + "s")
	if err != nil {
		panic(err)
	}
	c.TimeoutDuration = timeoutDuration
}

func (c *Context) Deadline() time.Time {
	return c.Start.Add(c.TimeoutDuration)
}

func (c *Context) HasExpired() bool {
	return time.Now().After(c.Deadline())
}

func (c *Context) Message(msg string) string {
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	msg = c.FnName + "[\"" + c.Version + "\"] " + msg
	return msg
}

func newContext(config *FunctionConfig, startTime time.Time, request *http.Request) *Context {
	context := &Context{
		RequestID:          uuid.New().String(),
		FnName:             *config.FunctionName,
		InvokedFunctionArn: *config.FunctionArn,
		Version:            *config.Version,
		MemSize:            strconv.Itoa(int(*config.MemorySize)),
		Timeout:            strconv.Itoa(int(*config.Timeout)),
		AccountID:          strconv.Itoa(config.AccountID),
		Start:              startTime,
		ClientContext:      request.Header.Get("X-Amz-Client-Context"),
		InitEnd:            time.Now(),
	}
	context.ParseTimeout()
	return context
}
