package sam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/viant/afs"
	"github.com/viant/cloudless/container/aws/lambda"
	"github.com/viant/cloudless/gateway/aws/apigw"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
)

type Service struct {
	fs      afs.Service
	lambda  *lambda.Service
	gateway *apigw.Service
	server  *http.Server
	config  *Config
}

func (s *Service) Start() {
	s.lambda.Start()
	mux := &http.ServeMux{}

	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		var body []byte
		if request.Body != nil {
			body, _ = ioutil.ReadAll(request.Body)
			request.Body.Close()
			request.Body = ioutil.NopCloser(bytes.NewReader(body))
		}

		s.logRequest(request, body)
		httpWriter := NewWriter()
		s.gateway.Do(httpWriter, request)
		s.logResponse(httpWriter)
		httpWriter.Update(writer)

	})
	s.server = &http.Server{
		Addr:    ":" + strconv.Itoa(s.config.Port),
		Handler: mux,
	}
	s.shutdownOnInterrupt()
	fmt.Printf("starting SAM endpoint: %v\n", s.config.Port)
	s.server.ListenAndServe()
}

//shutdownOnInterrupt
func (r *Service) shutdownOnInterrupt() {
	closed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		// We received an interrupt signal, shut down.
		if err := r.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}
		close(closed)
	}()
}

func (s *Service) Shutdown(ctx context.Context) error {
	if s.server != nil {
		s.server.Shutdown(ctx)
	}
	if s.lambda != nil {
		s.lambda.Shutdown()
	}
	return nil
}

func (s *Service) logRequest(request *http.Request, body []byte) {
	fmt.Printf("[%v] %v\n", request.Method, request.RequestURI)
	header, _ := json.Marshal(request.Header)
	if len(header) > 0 {
		fmt.Printf("[Header] %s\n", header)
	}
	if len(body) > 0 {
		fmt.Printf("[Body] %s\n", body)
	}
}

func (s *Service) logResponse(response *ProxyResponse) {
	fmt.Printf("[Status] %v\n", response.StatusCode)
	header, _ := json.Marshal(response.header)
	if len(header) > 0 {
		fmt.Printf("[Header] %s\n", header)
	}
	if response.Len() > 0 {
		if data, _ := ioutil.ReadAll(&response.Buffer); len(data) > 0 {
			fmt.Printf("[Body] %s\n", data)
		}
	}

}

func New(tmpl *Template, cfg *Config) (*Service, error) {
	ret := &Service{fs: afs.New(), config: cfg}
	cfg.Init()
	lambdaCfg, err := tmpl.LambdaConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load lambda service:%v", err)
	}
	ret.lambda = lambda.New(lambdaCfg)
	routes, err := tmpl.Routes()
	if err != nil {
		return nil, fmt.Errorf("failed to load routes: %v", err)
	}
	router := apigw.NewRouter(routes)
	ret.gateway = apigw.New(router, &apigw.Config{
		Endpoint: cfg.Endpoint,
		Region:   cfg.Region,
		AWS: &aws.Config{
			Region:   aws.String(cfg.Region),
			Endpoint: aws.String(cfg.Endpoint)},
	})
	return ret, nil
}
