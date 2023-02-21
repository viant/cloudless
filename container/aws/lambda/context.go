package lambda

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/cloudless/container/aws/lambda/shared"
	"log"
	"net/http"
	"strconv"
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
	Done               chan bool
	MaxMem             uint64
	InvocationType     string
	LogType            string
	LogTail            string // base64 encoded tail, no greater than 4096 bytes
	ErrorType          string // Unhandled vs Handled
	Ended              bool
	Ignore             bool
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

func (c *Context) TimeoutErr() error {
	return fmt.Errorf("%s %s Task timed out after %s.00 seconds", time.Now().Format("2006-01-02T15:04:05.999Z"),
		c.RequestID, c.Timeout)
}

func (c *Context) SetLogTail(r *http.Request) {
	//defer logsBuf.Reset()
	//
	//c.LogTail = ""
	//
	//if c.LogType != "Tail" {
	//	return
	//}
	//if noBootstrap {
	//	c.LogTail = r.Header.Get("Docker-Lambda-Log-Result")
	//	return
	//}
	//
	//// This is very annoying but seems to be necessary to ensure we get all the stdout/stderr from the subprocess
	//time.Sleep(1 * time.Millisecond)
	//
	//logs := logsBuf.Bytes()
	//
	//if len(logs) == 0 {
	//	return
	//}
	//
	//if len(logs) > 4096 {
	//	logs = logs[len(logs)-4096:]
	//}
	//c.LogTail = base64.StdEncoding.EncodeToString(logs)
}

func (c *Context) SetInitEnd(r *http.Request) {
	invokeWaitHeader := r.Header.Get("Docker-Lambda-Invoke-Wait")
	if invokeWaitHeader != "" {
		invokeWaitMs, err := strconv.ParseInt(invokeWaitHeader, 10, 64)
		if err != nil {
			log.Fatal(fmt.Errorf("Could not parse Docker-Lambda-Invoke-Wait header as int. Error: %s", err))
			return
		}
		c.InvokeWait = time.Unix(0, invokeWaitMs*int64(time.Millisecond))
	}
	initEndHeader := r.Header.Get("Docker-Lambda-Init-End")
	if initEndHeader != "" {
		initEndMs, err := strconv.ParseInt(initEndHeader, 10, 64)
		if err != nil {
			log.Fatal(fmt.Errorf("Could not parse Docker-Lambda-Init-End header as int. Error: %s", err))
			return
		}
		c.InitEnd = time.Unix(0, initEndMs*int64(time.Millisecond))
	}
}

func (c *Context) SetError(exitErr error) {
	responseErr := shared.Error{
		Message: exitErr.Error(),
		Type:    fmt.Sprintf("%T", exitErr),
	}
	if responseErr.Type == "errorString" {
		responseErr.Type = ""
		if responseErr.Message == "unexpected EOF" {
			responseErr.Message = "RequestId: " + c.RequestID + " Process exited before completing request"
		}
	} else if responseErr.Type == "ExitError" {
		responseErr.Type = "Runtime.ExitError" // XXX: Hack to add 'Runtime.' to error type
	}

	debug("Setting Reply in SetError")
	debug(responseErr)
	//if c.Reply == nil {
	//	c.Reply = &invoke.Response{Error: &responseErr}
	//} else {
	//	c.Reply.Error = &responseErr
	//}
}

func (c *Context) EndInvoke(exitErr error) {
	//debug("EndInvoke()")
	//if c.Ended {
	//	return
	//}
	//c.Ended = true
	//if exitErr != nil {
	//	debug(exitErr)
	//	c.SetError(exitErr)
	//} else if (c.Reply == nil || c.Reply.Error == nil) && c.HasExpired() {
	//	c.Reply = &invokeResponse{
	//		Error: &lambdaError{
	//			Message: c.TimeoutErr().Error(),
	//		},
	//	}
	//}
	if c.InitEnd.IsZero() {
		c.LogStartRequest()
	}

	c.LogEndRequest()

	if exitErr == nil {
		c.Done <- true
	}
}

func (c *Context) LogStartRequest() {
	c.InitEnd = time.Now()
	//systemLog("START RequestId: " + c.RequestID + " Version: " + c.Version)
}

func (c *Context) LogEndRequest() {
	//maxMem, _ := allProcsMemoryInMb()
	//if maxMem > c.MaxMem {
	//	c.MaxMem = maxMem
	//}
	//
	//diffMs := math.Min(float64(time.Now().Sub(c.InitEnd).Nanoseconds()),
	//	float64(c.TimeoutDuration.Nanoseconds())) / float64(time.Millisecond)
	//
	//initStr := ""
	//if !initPrinted {
	//	proc1stat, _ := os.Stat("/proc/1")
	//	processStartTime := proc1stat.ModTime()
	//	if c.InvokeWait.IsZero() {
	//		c.InvokeWait = serverInitEnd
	//	}
	//	if c.InvokeWait.Before(processStartTime) {
	//		c.InvokeWait = processStartTime
	//	}
	//	initDiffNs := c.InvokeWait.Sub(proc1stat.ModTime()).Nanoseconds() + c.InitEnd.Sub(c.Start).Nanoseconds()
	//	initDiffMs := math.Min(float64(initDiffNs), float64(c.TimeoutDuration.Nanoseconds())) / float64(time.Millisecond)
	//	initStr = fmt.Sprintf("Init Duration: %.2f ms\t", initDiffMs)
	//	initPrinted = true
	//}
	//
	//systemLog("END RequestId: " + c.RequestID)
	//systemLog(fmt.Sprintf(
	//	"REPORT RequestId: %s\t"+
	//		initStr+
	//		"Duration: %.2f ms\t"+
	//		"Billed Duration: %.f ms\t"+
	//		"Memory Size: %s MB\t"+
	//		"Max Memory Used: %d MB\t",
	//	c.RequestID, diffMs, math.Ceil(diffMs), c.MemSize, c.MaxMem))
}

func newContext(config *FunctionConfig) *Context {
	context := &Context{
		RequestID:          uuid.New().String(),
		FnName:             *config.FunctionName,
		InvokedFunctionArn: *config.FunctionArn,
		Version:            *config.Version,
		MemSize:            strconv.Itoa(int(*config.MemorySize)),
		Timeout:            strconv.Itoa(int(*config.Timeout)),
		AccountID:          strconv.Itoa(config.AccountID),
		Start:              time.Now(),
		Done:               make(chan bool),
	}
	context.ParseTimeout()
	return context
}
