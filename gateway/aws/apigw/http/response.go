package http

import (
	"bytes"
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"io/ioutil"
	"net/http"
)

func NewResponse(proxy *events.APIGatewayProxyResponse) (*http.Response, error) {
	var body = []byte(proxy.Body)
	var err error
	if proxy.IsBase64Encoded {
		if body, err = base64.StdEncoding.DecodeString(proxy.Body); err != nil {
			return nil, err
		}
	}
	response := &http.Response{StatusCode: proxy.StatusCode}
	response.Body = ioutil.NopCloser(bytes.NewReader(body))
	response.Header = http.Header{}
	if len(proxy.Headers) > 0 {
		for k, v := range proxy.Headers {
			response.Header.Set(k, v)
		}
	}
	return response, nil
}
