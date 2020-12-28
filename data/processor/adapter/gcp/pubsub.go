package gcp

import (
	"bytes"
	"encoding/base64"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
	"time"
)

//PubSubMessage represents PubSub message
type PubSubMessage struct {
	Data        string            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	MessageId   string            `json:"messageId"`
	PublishTime *time.Time        `json:"publishTime"`
	OrderingKey string            `json:"orderingKey"`
}

//NewRequest creates processing request
func (m PubSubMessage) NewRequest() (*processor.Request, error) {
	sourceURL := ""
	if len(m.Attributes) > 0 {
		//Source attribute is expected to store source URL
		sourceURL = m.Attributes["Source"]
	}
	data := []byte(m.Data)
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
			"PubSubMessage": m,
		},
		SourceURL: sourceURL,
		StartTime: time.Now(),
	}
	return request, nil
}
