package lambda

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Service struct {
	Config *Config
	io     bytes.Buffer
	fn     map[string]*Function
	lock   sync.Mutex
	server http.Server
	inited int32
}

func (s *Service) Init() {
	if !atomic.CompareAndSwapInt32(&s.inited, 0, 1) {
		return
	}
	router := &Router{}
	mux := router.Configure(s)
	s.server = http.Server{
		Addr:         fmt.Sprintf(":%v", s.Config.Port),
		Handler:      mux,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	}
	for i := range s.Config.Functions {
		s.Config.Functions[i].Init(s.Config)
	}
	s.enabledSighup()
}

func (s *Service) enabledSighup() {
	sighupReceiver := make(chan os.Signal, 1)
	signal.Notify(sighupReceiver, syscall.SIGHUP)
	go func() {
		<-sighupReceiver
		fmt.Fprintln(os.Stderr, "sighup received, exiting runtime...")
		s.Shutdown()
		os.Exit(2)
	}()
}

func (s *Service) Handle(writer http.ResponseWriter, request *http.Request) {
	funcName, method := URI(request.RequestURI).Info()
	var err error
	switch method {
	case "invocations":
		err = s.invokeFunction(funcName, writer, request)
	default:
		err = fmt.Errorf("unsuppored URI: %v", request.RequestURI)
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Service) Function(functionName string) (*Function, error) {
	fn, ok := s.fn[functionName]
	if ok {
		return fn, nil
	}
	fnConfig := s.Config.Lookup(functionName)
	if fnConfig == nil {
		return nil, fmt.Errorf("unknown function: %v", functionName)
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	fn, err := NewFunction(fnConfig)
	if err != nil {
		return nil, err
	}
	port := s.Config.nextPort()
	if err = fn.Start(context.Background(), port); err != nil {
		return nil, err
	}
	s.fn[functionName] = fn
	return fn, nil
}

func (s *Service) Start() {
	s.Init()
	go s.server.ListenAndServe()
}

func (s *Service) invokeFunction(name string, writer http.ResponseWriter, request *http.Request) error {
	startTime := time.Now()
	fn, err := s.Function(name)
	if err != nil {
		return err
	}
	lCtx := newContext(fn.Config, startTime, request)
	defer logEndRequest(lCtx, fn.LogStream)
	logStartRequest(lCtx, fn.LogStream)
	payload, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return err
	}
	deadline := lCtx.Deadline()
	invokeRequest := &messages.InvokeRequest{
		RequestId:          lCtx.RequestID,
		XAmznTraceId:       lCtx.XAmznTraceID,
		InvokedFunctionArn: lCtx.InvokedFunctionArn,
		Deadline: messages.InvokeRequest_Timestamp{
			Seconds: deadline.Unix(),
			Nanos:   int64(deadline.Nanosecond()),
		},
		ClientContext: []byte(lCtx.ClientContext),
		Payload:       payload,
	}

	logInvokeStart(lCtx, fn.LogStream)
	response, err := fn.Call(context.Background(), invokeRequest)
	if err != nil {
		fn.Stop()
		s.lock.Lock()
		delete(s.fn, name)
		s.lock.Unlock()
		lCtx.InvokeError = err.Error()
		logInvokeError(lCtx, fn.LogStream)
		return err
	}
	lCtx.InvokeResponse = response.Payload
	logInvokeEnd(lCtx, fn.LogStream)
	if _, err = writer.Write(response.Payload); err == io.EOF {
		err = nil
	}
	return err
}

func (s *Service) Shutdown() {
	for _, fn := range s.fn {
		_ = fn.Stop()
	}
	s.server.Shutdown(context.Background())
}

//New creates a service
func New(config *Config) *Service {
	config.Init()
	return &Service{
		Config: config,
		io:     bytes.Buffer{},
		fn:     map[string]*Function{},
		lock:   sync.Mutex{},
	}
}
