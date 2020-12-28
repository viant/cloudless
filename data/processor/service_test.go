package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestService_Process(t *testing.T) {
	useCases := []struct {
		ctx              context.Context
		description      string
		config           *Config
		request          *Request
		expectedResponse string
		expectedData     string
		Processor
		expectedRetryData string
	}{
		{
			description: "Summing up numbers concurrently",
			config: &Config{Concurrency: 5,
				DestinationURL: "mem://localhost/dest/sum-$UUID.txt",
				MaxExecTimeMs:  2000,
				RetryURL:       "mem://localhost/tmp/retry/",
				FailedURL:      "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{fs: afs.New()},
			ctx:       context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse: `{"Status":"ok", "Processed":10,"DestinationURL":"/mem://localhost/dest/sum/" }`,
			expectedData:     "45",
		},

		{
			description: "Summing up numbers concurrently in batches",
			config: &Config{Concurrency: 5,
				DestinationURL: "mem://localhost/dest/sum-$UUID.txt",
				MaxExecTimeMs:  2000,
				BatchSize:      1,
				RetryURL:       "mem://localhost/tmp/retry/",
				FailedURL:      "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{fs: afs.New()},
			ctx:       context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse: `{"Status":"ok", "Processed":10,"DestinationURL":"/mem://localhost/dest/sum/" }`,
			expectedData:     "45",
		},
		{
			description: "Summing up numbers concurrently with deadline ",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       2000,
				RetryURL:            "mem://localhost/tmp/replay/",
				FailedURL:           "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{
				fs:            afs.New(),
				sleepOnNumber: 8,
				sleepTime:     5 * time.Second,
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse:  `{"Status":"ok|retry", "Processed":9,"DestinationURL":"mem://localhost/dest/sum.txt", "RetriableErrors":1 }`,
			expectedData:      "37",
			expectedRetryData: "8",
		},

		{
			description: "Summing ordered  number ",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       200000,
				RetryURL:            "mem://localhost/tmp/replay/",
				FailedURL:           "mem://localhost/tmp/failed/",
				Sort: Sort{
					Spec: Spec{Format: "csv"},
					By: []Field{
						{
							Index: 0,
						},
					},
					Batch: true,
				},
			},
			Processor: &sumProcessor{
				fs: afs.New(),
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
1
1
1
2
2
2
1
3
3
3
1`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse: `{"Status":"ok", "Processed":3,"DestinationURL":"mem://localhost/dest/sum.txt"}`,
			expectedData:     "21",
		},

		{
			description: "Summing groupped number ",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       200000,
				RetryURL:            "mem://localhost/tmp/replay/",
				FailedURL:           "mem://localhost/tmp/failed/",
				Sort: Sort{
					Spec: Spec{Format: "csv"},
					By: []Field{
						{
							Index: 0,
						},
					},
					Batch: true,
				},
				BatchSize: 3,
			},
			Processor: &sumProcessor{
				fs: afs.New(),
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
1
1
1
2
2
2
1
3
3
3
1`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse: `{"Status":"ok", "Processed":4,"DestinationURL":"mem://localhost/dest/sum.txt"}`,
			expectedData:     "21",
		},

		{
			description: "Summing up numbers concurrently with deadline plus fail retry",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       2000,
				RetryURL:            "mem://localhost/tmp/retry/",
				MaxRetries:          5,
				FailedURL:           "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{
				fs:            afs.New(),
				sleepOnNumber: 8,
				sleepTime:     5 * time.Second,
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0`), nil, "mem://localhost/output/data/sum-retry05.txt"),
			expectedResponse:  `{"Status":"ok|retry", "Processed":9,"DestinationURL":"mem://localhost/dest/sum.txt" }`,
			expectedData:      "37",
			expectedRetryData: "8",
		},

		{
			description: "Summing up numbers concurrently with processing error",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       2000,
				RetryURL:            "mem://localhost/tmp/retry/",
				MaxRetries:          5,
				FailedURL:           "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{
				fs:            afs.New(),
				errorOnNumber: 3,
				err:           errors.New("test"),
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0`), nil, "mem://localhost/output/data/sum-retry05.txt"),
			expectedResponse:  `{"Status":"ok|retry", "Processed":9,"DestinationURL":"mem://localhost/dest/sum.txt" }`,
			expectedData:      "42",
			expectedRetryData: "3",
		},

		{
			description: "Summing up data from datastore",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       2000,
				RetryURL:            "mem://localhost/tmp/retry/",
				MaxRetries:          5,
				FailedURL:           "mem://localhost/tmp/failed/",
			},
			Processor: &dataStoreSumProcessor{
				dataStore: &fakerDataStore{
					cache: map[string]interface{}{
						"2": 100, "3": 600, "4": 1000,
					},
				},
				fs: afs.New(),
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
`), nil, "mem://localhost/output/data/sum-retry05.txt"),
			expectedResponse: `{"Status":"ok", "Processed":3,"DestinationURL":"mem://localhost/dest/sum.txt" }`,
			expectedData:     "700",
		},
		{
			description: "Summing up numbers concurrently with deadline and all workers sleeping ",
			config: &Config{Concurrency: 5,
				DestinationURL:      "mem://localhost/dest/sum.txt",
				DeadlineReductionMs: 500,
				MaxExecTimeMs:       2000,
				RetryURL:            "mem://localhost/tmp/replay/",
				FailedURL:           "mem://localhost/tmp/failed/",
			},
			Processor: &sumProcessor{
				fs:        afs.New(),
				allSleep:  true,
				sleepTime: 5 * time.Second,
			},
			ctx: context.Background(),
			request: NewRequest(strings.NewReader(`1
2
3
4
5
6
7
8
9
0
11`), nil, "mem://localhost/output/data/numbers.txt"),
			expectedResponse:  `{"Status":"ok|retry", "Processed":0,"DestinationURL":"mem://localhost/dest/sum.txt", "RetriableErrors":5 }`,
			expectedData:      "0",
			expectedRetryData: "0,1,11,2,3,4,5,6,7,8,9",
		},
	}
	fs := afs.New()
	for _, useCase := range useCases {
		useCase.request.StartTime = time.Now()
		maxExec := time.Duration(useCase.config.MaxExecTimeMs) * time.Millisecond
		var cancel context.CancelFunc
		if useCase.config.MaxExecTimeMs > 0 {
			useCase.ctx, cancel = context.WithTimeout(useCase.ctx, maxExec)
		}
		srv := New(useCase.config, fs, useCase.Processor, NewReporter)
		report := srv.Do(useCase.ctx, useCase.request)
		actual := report.BaseResponse()
		if cancel != nil {
			cancel()
		}
		if maxExec > 0 {
			assert.True(t, time.Duration(actual.RuntimeMs)*time.Millisecond <= maxExec, useCase.description)
		}

		if !assertly.AssertValues(t, useCase.expectedResponse, actual, useCase.description) {
			toolbox.DumpIndent(actual, true)
		}
		if actual.DestinationURL != "" {
			assert.True(t, !strings.Contains(actual.DestinationURL, "$"), useCase.description)
			data, err := fs.DownloadWithURL(useCase.ctx, actual.DestinationURL)
			assert.Nil(t, err, useCase.description)
			assert.Equal(t, useCase.expectedData, string(data), useCase.description)

		}
		if useCase.expectedRetryData != "" {
			data, err := fs.DownloadWithURL(useCase.ctx, actual.RetryURL)
			assert.Nil(t, err, useCase.description)
			strSlice := strings.Split(string(data), "\n")
			sort.Strings(strSlice)
			assert.Equal(t, useCase.expectedRetryData, strings.Join(strSlice, ","), useCase.description)
		}

	}
}

type sumKey string
type sumProcessor struct {
	fs afs.Service

	sleepTime     time.Duration
	sleepOnNumber int32
	allSleep      bool

	errorOnNumber int32
	err           error
}

//Pre runs preprocessing logic
func (p *sumProcessor) Pre(ctx context.Context, reporter Reporter) (context.Context, error) {
	var sum int32
	return context.WithValue(ctx, sumKey("sum"), &sum), nil
}

func (p *sumProcessor) Process(ctx context.Context, data []byte, reporter Reporter) error {
	value := ctx.Value(sumKey("sum"))
	sum := value.(*int32)
	text := string(data)
	items := strings.Split(text, "\n")
	for _, item := range items {
		if err := p.sum(item, sum); err != nil {
			return err
		}
	}
	return nil
}

func (p *sumProcessor) sum(data string, sum *int32) error {
	aNumber := int32(toolbox.AsInt(data))
	if p.errorOnNumber == aNumber {
		return p.err
	}
	if p.allSleep || p.sleepOnNumber == aNumber {
		time.Sleep(p.sleepTime)
	}
	atomic.AddInt32(sum, aNumber)
	return nil
}

func (p *sumProcessor) Post(ctx context.Context, reporter Reporter) error {
	destURL := reporter.BaseResponse().DestinationURL
	if destURL != "" {
		value := ctx.Value(sumKey("sum"))
		sum := value.(*int32)
		if err := p.fs.Upload(ctx, destURL, file.DefaultFileOsMode, strings.NewReader(fmt.Sprintf("%v", *sum))); err != nil {
			return err
		}
	}
	return nil
}

type fakerDataStore struct {
	cache map[string]interface{}
}

func (f *fakerDataStore) Get(key string) interface{} {
	return f.cache[key]
}

type dataStore interface {
	Get(key string) interface{}
}

type dataStoreSumProcessor struct {
	dataStore
	fs afs.Service
}

func (p *dataStoreSumProcessor) Pre(ctx context.Context, reporter Reporter) (context.Context, error) {
	var sum int32
	return context.WithValue(ctx, sumKey("sum"), &sum), nil
}

func (p *dataStoreSumProcessor) Process(ctx context.Context, data []byte, reporter Reporter) error {
	value := ctx.Value(sumKey("sum"))
	sum := value.(*int32)
	return p.sum(string(data), sum)
}

func (p *dataStoreSumProcessor) sum(key string, sum *int32) error {
	data := p.dataStore.Get(key)
	aNumber := int32(toolbox.AsInt(data))
	atomic.AddInt32(sum, aNumber)
	return nil
}

func (p *dataStoreSumProcessor) Post(ctx context.Context, reporter Reporter) error {
	destURL := reporter.BaseResponse().DestinationURL
	if destURL != "" {
		value := ctx.Value(sumKey("sum"))
		sum := value.(*int32)
		if err := p.fs.Upload(ctx, destURL, file.DefaultFileOsMode, strings.NewReader(fmt.Sprintf("%v", *sum))); err != nil {
			return err
		}
	}
	return nil
}
