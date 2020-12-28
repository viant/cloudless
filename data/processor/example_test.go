package processor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/toolbox"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

type sumKeyType string

const sumKey = sumKeyType("sum")

//SumProcessor represents sum processor
type SumProcessor struct{}

//Process sums coma separated numbers
func (p SumProcessor) Process(ctx context.Context, data []byte, reporter processor.Reporter) error {
	if len(data) == 0 {
		return nil
	}
	value := ctx.Value(sumKey)
	sum := value.(*int64)
	aNumber, err := toolbox.ToInt(string(data))
	if err != nil {
		return processor.NewDataCorruption(fmt.Sprintf("invalid number: %s, %v", data, err))
	}
	atomic.AddInt64(sum, int64(aNumber))
	return nil
}

func ExampleService_Do() {
	service := processor.New(&processor.Config{
		CorruptionURL: "mem://localhost/corrupted",
		RetryURL:      "mem://localhost/retry",
		FailedURL:     "mem://localhost/failed",
	}, afs.New(), &SumProcessor{}, processor.NewReporter)
	sum := int64(0)
	ctx := context.WithValue(context.Background(), sumKey, &sum)
	reporter := service.Do(ctx, processor.NewRequest(strings.NewReader("1\n2\n3\n5\nasd\n373\n23"),
		nil,
		"mem://localhost/response/numbers.txt"))
	fmt.Printf("Sum: %v\n", sum)
	//Prints sum 407
	response, _ := json.Marshal(reporter)
	fmt.Printf("%s\n", response)
	/* Prints
	{
		"CorruptionErrors": 1,
		"CorruptionURL": "mem://localhost/corrupted/response/numbers.txt",
		"Errors": [
			"invalid number: asd, strconv.ParseInt: parsing \"asd\": invalid syntax"
		],
		"Loaded": 7,
		"Processed": 6,
		"RetriableErrors": 0,
		"RetryErrors": 0,
		"RetryURL": "mem://localhost/retry/response/numbers-retry01.txt",
		"RuntimeMs": 1,
		"Status": "ok"
	}
	*/
}

//URLReporter represents URL reporter
type URLReporter struct {
	processor.BaseReporter
	ByResponseCode map[int]int
	mutex          sync.Mutex
}

//NewURLReporter represents URL reporeter
func NewURLReporter() processor.Reporter {
	return &URLReporter{
		ByResponseCode: make(map[int]int),
		BaseReporter: processor.BaseReporter{
			Response: &processor.Response{Status: processor.StatusOk},
		},
	}
}

type HTTPProcessor struct {
	BaseURL string
}

func (p HTTPProcessor) Process(ctx context.Context, data []byte, reporter processor.Reporter) error {
	urlReporter := reporter.(*URLReporter)
	URL := p.BaseURL + string(data)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, URL, nil)
	if err != nil {
		return processor.NewDataCorruption(fmt.Sprintf("invalid request: %v", URL))
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	urlReporter.mutex.Lock()
	defer urlReporter.mutex.Unlock()
	urlReporter.ByResponseCode[response.StatusCode]++
	return nil
}

func ExampleBaseReporter_BaseResponse() {
	ctx := context.Background()
	service := processor.New(&processor.Config{
		CorruptionURL: "mem://localhost/corrupted",
		RetryURL:      "mem://localhost/retry",
		FailedURL:     "mem://localhost/failed",
	}, afs.New(), &HTTPProcessor{BaseURL: "http://mydataexporter/enrich/?data="}, NewURLReporter)
	reporter := service.Do(ctx, processor.NewRequest(strings.NewReader("dGVzdCBpcyB0ZXN0\nYW5vdGhlciBvbmU="),
		nil,
		"mem://localhost/trigger/data.txt"))
	response, _ := json.Marshal(reporter)
	fmt.Printf("%s\n", response)
}
