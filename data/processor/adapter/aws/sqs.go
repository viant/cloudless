package aws

import (
	"bytes"
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
	"time"
)

//SQSEvent represents sns event
type SQSEvent struct {
	events.SQSEvent
}

//NewRequest creates processing request
func (e SQSEvent) NewRequest() (*processor.Request, error) {
	sourceURL := ""
	if len(e.Records[0].Attributes) > 0 {
		//Source attribute is expected to store source URL
		sourceURL = e.Records[0].Attributes["Source"]
	}
	data := []byte(e.Records[0].Body)
	if decoded, err := base64.StdEncoding.DecodeString(string(data)); err == nil {
		data = decoded
	}
	reader, err := ioutil.DataReader(bytes.NewReader(data), sourceURL)
	if err != nil {
		return nil, err
	}
	request := &processor.Request{
		ReadCloser: reader,
		Attrs: map[string]interface{}{
			"SQSMessage": e.Records[0],
		},
		SourceURL: sourceURL,
		StartTime: time.Now(),
	}
	return request, nil
}
