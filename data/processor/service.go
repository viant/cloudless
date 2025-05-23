package processor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/vc42/parquet-go"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/ioutil"
	"github.com/viant/gmetric"
	"github.com/viant/toolbox"
	"io"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Service represents processing service
type Service struct {
	Config  *Config
	Metrics *gmetric.Service
	fs      afs.Service
	Processor
	reporterProvider func() Reporter
}

// Do starts service processing
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
	var err error
	err = s.onMirror(context.Background(), request)
	if err != nil {
		response.LogError(err)
	}

	if s.Config.QuorumExt != "" {
		if notInQuorum, err := s.handleQuorumFlow(ctx, request, response); notInQuorum || err != nil {
			if err != nil {
				response.LogError(err)
			}
			return reporter
		}
	}

	if request.SourceType == Parquet {
		err = s.do(ctx, request, reporter, s.loadParquetData)
	} else { // CSV and JSON
		err = s.do(ctx, request, reporter, s.loadData)
	}
	if err != nil {
		response.LogError(err)
	}
	if err == nil {
		err := s.onDone(context.Background(), request)
		response.LogError(err)
	}
	return reporter
}

func (s *Service) handleQuorumFlow(ctx context.Context, request *Request, response *Response) (bool, error) {
	ext := path.Ext(request.SourceURL)
	hasQuorum := strings.Contains(ext, s.Config.QuorumExt)
	if !hasQuorum {
		response.Status = "QuorumSkipped"
		return true, nil
	}
	if request.ReadCloser != nil {
		request.ReadCloser.Close()
	}
	parent, _ := url.Split(request.SourceURL, file.Scheme)
	objects, err := s.fs.List(ctx, parent)
	if err != nil {
		return true, err
	}
	toDelete, err := s.mergeFiles(ctx, request, objects)
	if err != nil {
		return false, err
	}
	for _, URL := range toDelete { //delete files that are now part of quorum
		_ = s.fs.Delete(ctx, URL)
	}
	response.SourceURL = request.SourceURL
	request.ReadCloser, err = s.fs.OpenURL(ctx, request.SourceURL)
	return false, err
}

func (s *Service) mergeFiles(ctx context.Context, request *Request, objects []storage.Object) ([]string, error) {
	mergedFileURL := strings.Replace(request.SourceURL, s.Config.QuorumExt, "", 1)
	var toDelete = []string{request.SourceURL}
	request.SourceURL = mergedFileURL
	writer, err := s.fs.NewWriter(ctx, mergedFileURL, file.DefaultFileOsMode)
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if object.IsDir() || strings.HasSuffix(object.Name(), s.Config.QuorumExt) {
			continue
		}
		if err = s.mergeFile(ctx, object, writer); err != nil {
			return nil, err
		}
		toDelete = append(toDelete, object.URL())

	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return toDelete, err
}

func (s *Service) mergeFile(ctx context.Context, object storage.Object, writer io.WriteCloser) error {
	reader, err := s.fs.OpenURL(ctx, object.URL())
	if err != nil {
		return err
	}
	dataReader, err := ioutil.DataReader(reader, object.URL())
	if err != nil {
		return err
	}
	defer func() {
		_ = dataReader.Close()
		_ = reader.Close()
	}()
	_, err = io.Copy(writer, dataReader)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) do(ctx context.Context, request *Request, reporter Reporter,
	load func(ctx context.Context, waitGroup *sync.WaitGroup, request *Request, stream chan interface{}, response *Response, retryWriter *Writer)) (err error) {
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
	streamSize := 10000*s.Config.Concurrency + 1
	waitGroup.Add(s.Config.Concurrency + 1)
	stream := make(chan interface{}, streamSize)
	defer s.closeWriters(response, retryWriter, corruptionWriter)

	go load(ctx, waitGroup, request, stream, response, retryWriter)
	//fmt.Printf("!!!!!@@@%s!!!!!@@@\n", s.Config.Mode)
	//time.Sleep(35 * time.Second)

	start1 := time.Now()
	processed := make([]int32, s.Config.Concurrency)

	switch s.Config.Mode {
	case SafeCtxMode:
		if _, ok := ctx.Deadline(); !ok {
			deadline := s.Config.Deadline(ctx)
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Until(deadline))
			defer cancel()
		}

		fmt.Printf("AAA ###$$$ s.Config.TestCaseNr == %v\n", s.Config.TestCaseNr)
		fmt.Printf("AAA ###$$$ s.Config.Concurrency == %v\n", s.Config.Concurrency)

		for i := 0; i < s.Config.Concurrency; i++ {
			switch s.Config.TestCaseNr {
			case 1:
				go s.runWorker1(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter, &processed[i]) // 512 - QPS ~1533
			case 2:
				go s.runWorker2(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter)
			case 3:
				go s.runWorker3(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter)
			case 4:
				go s.runWorker4(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter)
			case 5:
				go s.runWorker5(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter)
			default:
				go s.runWorkerInSafeCtxMode(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter, &processed[i])
			}
		}
	default:
		var timeout = make(chan bool)

		go s.setTimeoutChannel(ctx, timeout)
		for i := 0; i < s.Config.Concurrency; i++ {
			go s.runWorker(ctx, waitGroup, stream, reporter, retryWriter, corruptionWriter, timeout)
		}
	}

	waitGroup.Wait()

	proceesingDuration := time.Since(start1)
	allCnt := 0
	for _, cnt := range processed {
		allCnt += int(cnt)
	}

	qps := float64(allCnt) / proceesingDuration.Seconds()
	fmt.Printf("AAA ###$$$ AVG QPS: %v, allCnt: %v, proceesingDuration: %v\n", qps, allCnt, proceesingDuration)

	if postProcess, ok := s.Processor.(PostProcessor); ok {
		if err = postProcess.Post(ctx, reporter); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) loadParquetData(ctx context.Context, waitGroup *sync.WaitGroup, request *Request, stream chan interface{}, response *Response, retryWriter *Writer) {
	defer waitGroup.Done()
	defer close(stream)
	deadline := s.Config.LoaderDeadline(ctx)
	parReader := parquet.NewReader(request.ReaderAt)
	defer parReader.Close()

	for {
		rowPtr := reflect.New(request.RowType).Interface()
		err := parReader.Read(rowPtr)
		if err != nil {
			if err != io.EOF {
				response.LogError(err)
			}
			break
		}
		if time.Now().After(deadline) {
			data, err := gojay.Marshal(rowPtr)
			if err != nil {
				response.LogError(err)
			} else {
				s.writeToRetry(retryWriter, data, response)
			}
			response.LoadTimeouts++
			continue
		}
		response.Loaded++
		stream <- rowPtr
	}
}

func (s *Service) loadData(ctx context.Context, waitGroup *sync.WaitGroup, request *Request, stream chan interface{}, response *Response, retryWriter *Writer) {
	defer waitGroup.Done()
	defer close(stream)
	var reader io.Reader = request.ReadCloser
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

	if request.SourceType == CSV && s.Config.Sort.Batch && len(s.Config.Sort.By) > 0 {
		s.loadInGroups(ctx, scanner, deadline, retryWriter, response, stream)
		return
	}
	if request.SourceType == CSV && s.Config.BatchSize > 0 {
		s.loadInBatches(ctx, s.Config.BatchSize, scanner, deadline, retryWriter, response, stream)
		return
	}

	scanDuration := time.Duration(0)

	s1 := time.Now()

	//for scanner.Scan() {
	for {
		start := time.Now()
		ok := scanner.Scan()
		scanDuration += time.Since(start)

		if !ok {
			break
		}

		bs := scanner.Bytes()
		data := make([]byte, len(bs))
		copy(data, bs)
		if time.Now().After(deadline) {
			s.writeToRetry(retryWriter, data, response)
			response.LoadTimeouts++
			continue
		}
		if request.SourceType == JSON && request.RowType != nil {
			rowPtr := reflect.New(request.RowType).Interface()
			if err := gojay.Unmarshal(data, rowPtr); err != nil {
				response.LogError(err)
				continue
			}
			stream <- rowPtr
		} else {
			stream <- data
		}
		response.Loaded++
	}

	f1 := time.Now()
	allDuration := f1.Sub(s1)
	fmt.Printf("AAA Scanner duration: scan: %v,  unmarshalling: %v, all: %v\n", scanDuration, allDuration-scanDuration, allDuration)

}

func (s *Service) loadData2(
	ctx context.Context,
	wg *sync.WaitGroup,
	req *Request,
	stream chan interface{},
	resp *Response,
	retryWriter *Writer,
) {
	defer wg.Done()
	defer close(stream)

	var reader io.Reader = req.ReadCloser
	if len(s.Config.Sort.By) > 0 {
		var err error
		if reader, err = s.sortInput(reader, resp); err != nil {
			resp.LogError(err)
			return
		}
	}

	deadline := s.Config.LoaderDeadline(ctx)
	bufReader := bufio.NewReaderSize(reader, 64*1024) // 64 KB buffer

	for {
		if ctx.Err() != nil || time.Now().After(deadline) {
			break
		}

		line, err := bufReader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				resp.LogError(err)
			}
			break
		}

		line = bytes.TrimSpace(line)         // optional
		data := append([]byte(nil), line...) // safe copy

		if time.Now().After(deadline) {
			s.writeToRetry(retryWriter, data, resp)
			resp.LoadTimeouts++
			continue
		}

		var item interface{} = data
		if req.SourceType == JSON && req.RowType != nil {
			rowPtr := reflect.New(req.RowType).Interface()
			if err := gojay.Unmarshal(data, rowPtr); err != nil {
				resp.LogError(err)
				continue
			}
			item = rowPtr
		}

		select {
		case stream <- item:
		case <-ctx.Done():
			resp.LogError(newProcessError("loader context canceled"))
			return
		}

		resp.Loaded++
	}
}

func (s *Service) loadData3(ctx context.Context, waitGroup *sync.WaitGroup, request *Request, stream chan interface{}, response *Response, retryWriter *Writer) {
	defer waitGroup.Done()
	defer close(stream)
	var reader io.Reader = request.ReadCloser
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

	if request.SourceType == CSV && s.Config.Sort.Batch && len(s.Config.Sort.By) > 0 {
		s.loadInGroups(ctx, scanner, deadline, retryWriter, response, stream)
		return
	}
	if request.SourceType == CSV && s.Config.BatchSize > 0 {
		s.loadInBatches(ctx, s.Config.BatchSize, scanner, deadline, retryWriter, response, stream)
		return
	}

	//for scanner.Scan() {
	//	bs := scanner.Bytes()
	//	data := make([]byte, len(bs))
	//	copy(data, bs)
	//	if time.Now().After(deadline) {
	//		s.writeToRetry(retryWriter, data, response)
	//		response.LoadTimeouts++
	//		continue
	//	}
	//	if request.SourceType == JSON && request.RowType != nil {
	//		rowPtr := reflect.New(request.RowType).Interface()
	//		if err := gojay.Unmarshal(data, rowPtr); err != nil {
	//			response.LogError(err)
	//			continue
	//		}
	//		stream <- rowPtr
	//	} else {
	//		stream <- data
	//	}
	//	response.Loaded++
	//}

	////deadline := s.Config.LoaderDeadline(ctx)
	//bufReader := bufio.NewReaderSize(reader, 1*1024*1024 /*64*1024*/) // 64 KB buffer
	//
	//for {
	//	if ctx.Err() != nil || time.Now().After(deadline) {
	//		break
	//	}
	//
	//	line, err := bufReader.ReadBytes('\n')
	//	if err != nil {
	//		if err != io.EOF {
	//			response.LogError(err)
	//		}
	//		break
	//	}
	//
	//	line = bytes.TrimSpace(line)         // optional
	//	data := append([]byte(nil), line...) // safe copy
	//
	//	if time.Now().After(deadline) {
	//		s.writeToRetry(retryWriter, data, response)
	//		response.LoadTimeouts++
	//		continue
	//	}
	//
	//	var item interface{} = data
	//	if request.SourceType == JSON && request.RowType != nil {
	//		rowPtr := reflect.New(request.RowType).Interface()
	//		if err := gojay.Unmarshal(data, rowPtr); err != nil {
	//			response.LogError(err)
	//			continue
	//		}
	//		item = rowPtr
	//	}
	//
	//	select {
	//	case stream <- item:
	//	case <-ctx.Done():
	//		response.LogError(newProcessError("loader context canceled"))
	//		return
	//	}
	//
	//	response.Loaded++
	//}

	//////////
	//	defer wg.Done()
	//	defer close(stream)

	//reader = bufio.NewReaderSize(request.ReadCloser, 64*1024)

	reader2 := bufio.NewReaderSize(reader, 1*1024*1024 /*64*1024*/) // 64 KB buffer

	deadline = s.Config.LoaderDeadline(ctx)

	// channel between reader and workers
	lineChan := make(chan []byte, s.Config.Concurrency*2)

	// Stage 1: reader goroutine
	go func() {
		defer close(lineChan)
		for {
			line, err := reader2.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					response.LogError(err)
				}
				break
			}
			data := append([]byte(nil), bytes.TrimSpace(line)...)

			if time.Now().After(deadline) {
				s.writeToRetry(retryWriter, data, response)
				response.LoadTimeouts++
				continue
			}
			select {
			case lineChan <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Stage 2: worker pool
	workerCount := s.Config.Concurrency
	var workerWG sync.WaitGroup
	workerWG.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer workerWG.Done()
			for data := range lineChan {
				var item interface{} = data
				if request.SourceType == JSON && request.RowType != nil {
					rowPtr := reflect.New(request.RowType).Interface()
					if err := gojay.Unmarshal(data, rowPtr); err != nil {
						response.LogError(err)
						continue
					}
					item = rowPtr
				}

				select {
				case stream <- item:
					atomic.AddInt32(&response.Loaded, 1)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Wait for workers to finish
	workerWG.Wait()
}

func (s *Service) runWorker(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer, timeout chan bool) {
	response := reporter.BaseResponse()
	defer wg.Done()
	deadline := s.Config.Deadline(ctx)
	for data := range stream {
		if time.Now().After(deadline) {
			s.retryWriter2(ctx, data, retryWriter, response)
			continue
		}
		var done = make(chan bool)
		go func() {
			err := s.Process(ctx, data, reporter)
			if err != nil {
				switch actual := err.(type) {
				case *DataCorruption:
					response.LogError(err)
					s.corruptionWriter(data, corruptionWriter, response)
				case *PartialRetry:
					s.partialRetryWriter(actual, data, response, retryWriter)
					response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
				default:
					response.LogError(newProcessError(fmt.Sprintf(" failed to process data due to %v, %+v", err, data)))
					s.retryWriter(data, retryWriter, response)
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
			response.LogError(newProcessError(fmt.Sprintf("deadline exceeded while processing %+v", data)))
			s.retryWriter(data, retryWriter, response)
		}

	}
}

func (s *Service) retryWriter2(ctx context.Context, data interface{}, retryWriter *Writer, response *Response) {
	v, ok := data.([]byte)
	if ok {
		if err := retryWriter.Write(ctx, v); err != nil {
			response.LogError(newRetryError(fmt.Sprintf(" failed to write data %v due to %v", data, err)))
		}
	} else {
		vj, err := gojay.Marshal(data)
		if err != nil {
			response.LogError(fmt.Errorf(" failed to marshal data %+v due to %v", data, err))
		} else {
			if err = retryWriter.Write(ctx, vj); err != nil {
				response.LogError(newRetryError(fmt.Sprintf(" failed to write data %v due to %v", vj, err)))
			}
		}
	}
}

func (s *Service) retryWriter(data interface{}, retryWriter *Writer, response *Response) {
	v, ok := data.([]byte)
	if ok {
		s.writeToRetry(retryWriter, v, response)
	} else {
		vj, err := gojay.Marshal(data)
		if err != nil {
			response.LogError(fmt.Errorf(" failed to marshal data %+v due to %v", data, err))
		} else {
			s.writeToRetry(retryWriter, vj, response)
		}
	}
}

func (s *Service) partialRetryWriter(actual *PartialRetry, data interface{}, response *Response, retryWriter *Writer) {
	v, ok := data.([]byte)
	if ok {
		if actual.data != nil {
			v = actual.data.([]byte)
			atomic.AddInt32(&response.Processed, 1)
		}
		s.writeToRetry(retryWriter, v, response)
	} else {
		if actual.data != nil {
			data = actual.data
		}
		vj, err := gojay.Marshal(data)
		if err != nil {
			response.LogError(fmt.Errorf(" failed to marshal data %+v due to %v", data, err))
		} else {
			atomic.AddInt32(&response.Processed, 1)
			s.writeToRetry(retryWriter, vj, response)
		}
	}
}

func (s *Service) corruptionWriter(data interface{}, corruptionWriter *Writer, response *Response) {
	v, ok := data.([]byte)
	if ok {
		s.writeCorrupted(corruptionWriter, v, response)
	} else {
		vj, err := gojay.Marshal(data)
		if err != nil {
			response.LogError(fmt.Errorf(" failed to marshal data %+v due to %v", data, err))
		} else {
			s.writeCorrupted(corruptionWriter, vj, response)
		}
	}
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
	response.Destination = s.Config.ExpandDestination(request.StartTime)

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
	if request.SourceType == Parquet {
		response.CorruptionURL = strings.Replace(response.CorruptionURL, ".parquet", ".json.gz", 1)
		response.RetryURL = strings.Replace(response.RetryURL, ".parquet", ".json.gz", 1)
	}
}

func (s *Service) StartMetricsEndpoint() {
	if s.Config.MetricPort == 0 {
		fmt.Printf("metric endpoint is off")
		return
	}
	mux := http.NewServeMux()
	mux.Handle(metricURI, gmetric.NewHandler(metricURI, s.Metrics))
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(s.Config.MetricPort),
		Handler: mux,
	}
	fmt.Printf("starting metric endpoint: %v", s.Config.MetricPort)
	go server.ListenAndServe()
}

func (s *Service) closeWriters(response *Response, retryWriter *Writer, corruptionWriter *Writer) {
	if retryWriter != nil {
		response.LogError(retryWriter.Close())
	}
	if corruptionWriter != nil {
		response.LogError(corruptionWriter.Close())
	}
}

func (s *Service) loadInBatches(ctx context.Context, batchSize int, scanner *bufio.Scanner, deadline time.Time, retryWriter *Writer, response *Response, stream chan interface{}) {
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

func (s *Service) loadInGroups(ctx context.Context, scanner *bufio.Scanner, deadline time.Time, retryWriter *Writer, response *Response, stream chan interface{}) {
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

func (s *Service) onMirror(ctx context.Context, request *Request) error {
	if s.Config.OnMirrorURL == "" || url.Scheme(request.SourceURL, "") == "" {
		return nil
	}
	urlPath := url.Path(request.SourceURL)
	mirrorURL := url.Join(s.Config.OnMirrorURL, urlPath)
	return s.fs.Copy(ctx, request.SourceURL, mirrorURL)
}

func (s *Service) sortInput(reader io.Reader, response *Response) (io.Reader, error) {
	return s.Config.Sort.Order(reader, s.Config)
}

// New creates data processing service
func New(config *Config, fs afs.Service, processor Processor, reporterProvider func() Reporter) *Service {
	return &Service{Config: config,
		Metrics:          gmetric.New(),
		fs:               fs,
		Processor:        processor,
		reporterProvider: reporterProvider,
	}
}

// NewWithMetrics creates data processing service
func NewWithMetrics(config *Config, fs afs.Service, processor Processor, reporterProvider func() Reporter, metrics *gmetric.Service) *Service {
	return &Service{Config: config,
		Metrics:          metrics,
		fs:               fs,
		Processor:        processor,
		reporterProvider: reporterProvider,
	}
}

// only ctx
func (s *Service) runWorkerInSafeCtxMode(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer, processed *int32) {
	defer wg.Done()
	response := reporter.BaseResponse()
	ctxErrLogged := false
	//var processed int32 = 0 //TODO

	for data := range stream {
		if err := ctx.Err(); err != nil {
			s.retryWriter2(ctx, data, retryWriter, response)
			if !ctxErrLogged {
				logError(err, response, data, &ctxErrLogged)
			}
			continue
		}

		err := s.Process(ctx, data, reporter)

		if err != nil {
			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				logError(err, response, data, &ctxErrLogged)
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			*processed++
		}
	}

	atomic.AddInt32(&response.Processed, *processed)

}

func logError(err error, response *Response, data interface{}, ctxErrLogged *bool) {
	if err == nil {
		return
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		response.LogError(newProcessError(fmt.Sprintf("deadline exceeded while processing %+v", data)))
		*ctxErrLogged = true
	case errors.Is(err, context.Canceled):
		response.LogError(newProcessError(fmt.Sprintf("context canceled while processing %+v", data)))
		*ctxErrLogged = true
	default:
		response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %v, %+v", err, data)))
	}
}

// no ctx, actx/request 500
func (s *Service) runWorker1(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer, processed *int32) {
	response := reporter.BaseResponse()

	defer wg.Done()
	//deadline := s.Config.Deadline(ctx)
	//start := time.Now()

	ctx = context.Background()

	for data := range stream {
		//if time.Now().After(deadline) {
		//	s.retryWriter2(ctx, data, retryWriter, response)
		//	continue
		//}

		err := s.Process(ctx, data, reporter)
		if err != nil {

			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				response.LogError(newProcessError(fmt.Sprintf(" failed to process data due to %v, %+v", err, data)))
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			*processed++ //TODO int64 + flushing
		}
	}

	atomic.AddInt32(&response.Processed, *processed)
	//finish := time.Now()
	//timeTaken := finish.Sub(start)
	//qps := float64(*processed) / timeTaken.Seconds()

	// fmt.Printf("###worker done - start: %s finish: %s processed %d items in %s QPS: %d \n", start, finish, *processed, time.Since(start), qps)
}

// ctx & deadline
func (s *Service) runWorker2(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer) {
	defer wg.Done()
	response := reporter.BaseResponse()
	ctxErrLogged := false
	deadline := s.Config.Deadline(ctx) // !
	var processed int32 = 0            //TODO

	for data := range stream {
		if time.Now().After(deadline) {
			s.retryWriter2(ctx, data, retryWriter, response)
			if !ctxErrLogged {
				err := fmt.Errorf("deadline exceeded while processing %+v", data)
				logError(err, response, data, &ctxErrLogged)
			}
			continue
		}

		err := s.Process(ctx, data, reporter)

		if err != nil {
			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				logError(err, response, data, &ctxErrLogged)
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			processed++
		}
	}

	atomic.AddInt32(&response.Processed, processed)

}

// actx per worker, not iter check
func (s *Service) runWorker3(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer) {
	defer wg.Done()
	response := reporter.BaseResponse()
	ctxErrLogged := false
	deadline := s.Config.Deadline(ctx)
	aCtx, cancel := context.WithTimeout(ctx, time.Until(deadline))
	defer cancel()
	var processed int32 = 0 //TODO

	for data := range stream {

		err := s.Process(aCtx, data, reporter)

		if err != nil {
			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				logError(err, response, data, &ctxErrLogged)
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			processed++
		}
	}

	atomic.AddInt32(&response.Processed, processed)

}

// actx per worker, actx iter check
func (s *Service) runWorker4(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer) {
	defer wg.Done()
	response := reporter.BaseResponse()
	ctxErrLogged := false
	deadline := s.Config.Deadline(ctx)
	aCtx, cancel := context.WithTimeout(ctx, time.Until(deadline))
	defer cancel()
	var processed int32 = 0 //TODO

	for data := range stream {
		if err := ctx.Err(); err != nil {
			s.retryWriter2(ctx, data, retryWriter, response)
			if !ctxErrLogged {
				logError(err, response, data, &ctxErrLogged)
			}
			continue
		}

		err := s.Process(aCtx, data, reporter)

		if err != nil {
			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				logError(err, response, data, &ctxErrLogged)
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			processed++
		}
	}

	atomic.AddInt32(&response.Processed, processed)

}

// actx per worker, deadline iter check
func (s *Service) runWorker5(ctx context.Context, wg *sync.WaitGroup, stream chan interface{}, reporter Reporter, retryWriter *Writer, corruptionWriter *Writer) {
	defer wg.Done()
	response := reporter.BaseResponse()
	ctxErrLogged := false
	deadline := s.Config.Deadline(ctx)
	aCtx, cancel := context.WithTimeout(ctx, time.Until(deadline))
	defer cancel()
	var processed int32 = 0 //TODO

	for data := range stream {
		if time.Now().After(deadline) {
			s.retryWriter2(ctx, data, retryWriter, response)
			if !ctxErrLogged {
				err := fmt.Errorf("deadline exceeded while processing %+v", data)
				logError(err, response, data, &ctxErrLogged)
			}
			continue
		}

		err := s.Process(aCtx, data, reporter)

		if err != nil {
			switch actual := err.(type) {
			case *DataCorruption:
				response.LogError(err)
				s.corruptionWriter(data, corruptionWriter, response)
			case *PartialRetry:
				s.partialRetryWriter(actual, data, response, retryWriter)
				response.LogError(newProcessError(fmt.Sprintf("failed to process data due to %+v,  %+v", actual, data)))
			default:
				logError(err, response, data, &ctxErrLogged)
				s.retryWriter(data, retryWriter, response)
			}
		} else {
			processed++
		}
	}

	atomic.AddInt32(&response.Processed, processed)

}
