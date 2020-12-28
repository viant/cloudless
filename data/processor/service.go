package processor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Service represents processing service
type Service struct {
	Config *Config
	fs     afs.Service
	Processor
	reporterProvider func() Reporter
}

//Do starts service processing
func (s *Service) Do(ctx context.Context, request *Request) Reporter {
	reporter := s.reporterProvider()
	response := reporter.BaseResponse()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = s.Config.LoaderDeadline(ctx)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deadline.Sub(time.Now()))
		defer cancel()
	}

	if request.StartTime.IsZero() {
		request.StartTime = time.Now()
	}
	response.SourceURL = request.SourceURL
	response.StartTime = request.StartTime
	defer func() {
		//use new context in case the other got deadline exceeded
		if err := s.onDone(context.Background(), request); err != nil {
			response.LogError(err)
		}
	}()
	err := s.do(ctx, request, reporter)
	if err != nil {
		response.LogError(err)
	}
	return reporter
}

func (s *Service) do(ctx context.Context, request *Request, reporter Reporter) (err error) {
	response := reporter.BaseResponse()
	s.makeURL(response, request)
	defer func() {
		response.RuntimeMs = int(time.Since(request.StartTime).Milliseconds())
	}()
	retryWriter, corruptionWriter := s.openWriters(response.RetryURL, response.CorruptionURL)
	if preProcess, ok := s.Processor.(PreProcessor); ok {
		if ctx, err = preProcess.Pre(ctx, reporter); err != nil {
			return err
		}
	}
	if s.Config.Concurrency == 0 {
		s.Config.Concurrency = 1
	}
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(s.Config.Concurrency + 1)
	stream := make(chan []byte)
	defer s.closeWriters(response, retryWriter, corruptionWriter)
	go s.loadSourceData(ctx, waitGroup, request.ReadCloser, stream, response, retryWriter)
	var timeout = make(chan bool)

	go s.setTimeoutChannel(ctx, timeout)
	for i := 0; i < s.Config.Concurrency; i++ {
		go s.runWorker(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter, timeout)
	}
	waitGroup.Wait()

	if postProcess, ok := s.Processor.(PostProcessor); ok {
		if err = postProcess.Post(ctx, reporter); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) setTimeoutChannel(ctx context.Context, timeout chan bool) {
	var remaining = s.Config.Deadline(ctx).Sub(time.Now())
	if remaining > 0 {
		select {
		case <-time.After(remaining):
			for i := 0; i < s.Config.Concurrency; i++ {
				select {
				case timeout <- true:
				case <-time.After(time.Millisecond):
				}
			}
			close(timeout)
		}
	}
}

func (s *Service) openWriters(retryURL, corruptionURL string) (*Writer, *Writer) {
	var retryWriter, corruptionWriter *Writer
	if retryURL != "" {
		retryWriter = NewWriter(retryURL, s.fs)
	}
	if corruptionURL != "" {
		corruptionWriter = NewWriter(corruptionURL, s.fs)
	}
	return retryWriter, corruptionWriter
}

func (s *Service) makeURL(response *Response, request *Request) {
	response.DestinationURL = s.Config.ExpandDestinationURL(request.StartTime)
	response.DestinationCodec = s.Config.DestinationCodec
	if s.Config.CorruptionURL != "" {
		response.CorruptionURL = expandURL(request.TransformSourceURL(s.Config.CorruptionURL), request.StartTime)
	}
	retryURL := s.Config.RetryURL
	if request.Retry() >= s.Config.MaxRetries {
		retryURL = s.Config.FailedURL
	}
	if retryURL == "" {
		return
	}
	retryURL = request.TransformSourceURL(retryURL)
	retryURL = expandRetryURL(retryURL, request.StartTime, request.Retry())
	response.RetryURL = retryURL
}

func (s *Service) closeWriters(response *Response, retryWriter *Writer, corruptionWriter *Writer) {
	if retryWriter != nil {
		response.LogError(retryWriter.Close())
	}
	if corruptionWriter != nil {
		response.LogError(corruptionWriter.Close())
	}
}

func (s *Service) loadSourceData(ctx context.Context, waitGroup *sync.WaitGroup, reader io.Reader, stream chan []byte, response *Response, retryWriter *Writer) {
	defer waitGroup.Done()
	defer close(stream)
	if len(s.Config.Sort.By) > 0 {
		var err error
		if reader, err = s.sortInput(reader, response); err != nil {
			response.LogError(err)
		}
	}
	deadline := s.Config.LoaderDeadline(ctx)
	scanner := bufio.NewScanner(reader)
	s.Config.AdjustScannerBuffer(scanner)

	defer func() {
		if scanner.Err() != io.EOF {
			response.LogError(scanner.Err())
		}
	}()

	if s.Config.Sort.Batch && len(s.Config.Sort.By) > 0 {
		s.loadInGroups(ctx, scanner, deadline, retryWriter, response, stream)
		return
	}
	if s.Config.BatchSize > 0 {
		s.loadInBatches(ctx, s.Config.BatchSize, scanner, deadline, retryWriter, response, stream)
		return
	}

	for scanner.Scan() {
		bs := scanner.Bytes()
		data := make([]byte, len(bs))
		copy(data, bs)
		if time.Now().After(deadline) {
			s.writeToRetry(retryWriter, data, response)
			response.LoadTimeouts++
			continue
		}
		response.Loaded++
		stream <- data
	}
}

func (s *Service) loadInBatches(ctx context.Context, batchSize int, scanner *bufio.Scanner, deadline time.Time, retryWriter *Writer, response *Response, stream chan []byte) {
	batch := make([][]byte, 0)
	for scanner.Scan() {
		bs := scanner.Bytes()
		data := make([]byte, len(bs))
		copy(data, bs)
		batch = append(batch, data)

		if time.Now().After(deadline) {
			s.writeToRetry(retryWriter, bytes.Join(batch, []byte("\n")), response)
			response.LoadTimeouts++
			batch = make([][]byte, 0)
			continue
		}
		response.Loaded++
		if len(batch) >= batchSize {
			stream <- bytes.Join(batch, []byte("\n"))
			batch = make([][]byte, 0)
			response.Batched++
		}
	}
	if len(batch) > 0 {
		stream <- bytes.Join(batch, []byte("\n"))
		response.Batched++
	}
}

func (s *Service) loadInGroups(ctx context.Context, scanner *bufio.Scanner, deadline time.Time, retryWriter *Writer, response *Response, stream chan []byte) {
	batch := make([][]byte, 0)
	groupValue := ""
	spec := &s.Config.Sort.Spec
	groupField := s.Config.Sort.By[0]
	flushGroup := false
	for scanner.Scan() {
		bs := scanner.Bytes()
		data := make([]byte, len(bs))
		copy(data, bs)

		nextValue := toolbox.AsString(groupField.Value(data, spec))
		if len(batch) == 0 {
			groupValue = nextValue
		} else if nextValue != groupValue {
			flushGroup = true
		}
		groupValue = nextValue
		if time.Now().After(deadline) {
			batch = append(batch, data)
			s.writeToRetry(retryWriter, bytes.Join(batch, []byte("\n")), response)
			response.LoadTimeouts++
			batch = make([][]byte, 0)
			continue
		}

		response.Loaded++
		if flushGroup {
			stream <- bytes.Join(batch, []byte("\n"))
			batch = make([][]byte, 0)
			response.Batched++
			flushGroup = false
		}
		batch = append(batch, data)
		if s.Config.BatchSize > 0 && len(batch) == s.Config.BatchSize {
			flushGroup = true
		}
	}
	if len(batch) > 0 {
		stream <- bytes.Join(batch, []byte("\n"))
		response.Batched++
	}
}

func (s *Service) runWorker(ctx context.Context, wg *sync.WaitGroup, stream chan []byte, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer, timeout chan bool) {
	response := reporter.BaseResponse()
	defer wg.Done()
	deadline := s.Config.Deadline(ctx)
	for data := range stream {
		if time.Now().After(deadline) {
			if err := retryWriter.Write(ctx, data); err != nil {
				response.LogError(newRetryError(fmt.Sprintf(" failed to write data %s due to %v", data, err)))
			}
			continue
		}
		var done = make(chan bool)
		go func() {
			err := s.Process(ctx, data, reporter)
			if err != nil {
				switch actual := err.(type) {
				case *DataCorruption:
					response.LogError(err)
					s.writeCorrupted(corruptionWriter, data, response)
				case *PartialRetry:
					if len(actual.data) > 0 {
						data = actual.data
						atomic.AddInt32(&response.Processed, 1)
					}
					response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %v,  %s", err, data)))
					s.writeToRetry(retryWriter, data, response)
				default:
					response.LogError(newProcessError(fmt.Sprintf(" failed to process data due to %v, %s", err, data)))
					s.writeToRetry(retryWriter, data, response)
				}
			} else {
				atomic.AddInt32(&response.Processed, 1)
			}
			done <- true
			close(done)
		}()

		select {
		case <-done:
		case <-timeout:
			response.LogError(newProcessError(fmt.Sprintf("deadline exceeded while processing %s", data)))
			s.writeToRetry(retryWriter, data, response)
		}

	}
}

func (s *Service) writeToRetry(writer *Writer, data []byte, response *Response) {
	if writer == nil {
		return
	}
	response.Skipped++
	if err := writer.Write(context.Background(), data); err != nil {
		response.LogError(newRetryError(fmt.Sprintf(" failed to write retry data %s due to %v", data, err)))
	}
}

func (s *Service) writeCorrupted(writer *Writer, data []byte, response *Response) {
	if writer == nil {
		return
	}
	if err := writer.Write(context.Background(), data); err != nil {
		response.LogError(newRetryError(fmt.Sprintf(" failed to write corrupted data %s due to %v", data, err)))
	}
}

func (s *Service) onDone(ctx context.Context, request *Request) error {
	if readerCloser := request.ReadCloser; readerCloser != nil {
		readerCloser.Close()
	}
	if s.Config.OnDone == "" || url.Scheme(request.SourceURL, "") == "" {
		return nil
	}
	switch strings.ToLower(s.Config.OnDone) {
	case OnDoneDelete:
		return s.fs.Delete(ctx, request.SourceURL)
	case OnDoneMove:
		urlPath := url.Path(request.SourceURL)
		destURL := url.Join(s.Config.OnDoneURL, urlPath)
		return s.fs.Move(ctx, request.SourceURL, destURL)
	}

	return nil
}

func (s *Service) sortInput(reader io.Reader, response *Response) (io.Reader, error) {
	return s.Config.Sort.Order(reader, s.Config)
}

// New creates a processing service
func New(config *Config, fs afs.Service, processor Processor, reporterProvider func() Reporter) *Service {
	return &Service{Config: config,
		fs:               fs,
		Processor:        processor,
		reporterProvider: reporterProvider,
	}
}
