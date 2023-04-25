package http

import (
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/gateway"
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Request http.Request

//Request converts to http.Request
//apigw doesn't include the function name in the URI segments
func (r *Request) ProxyRequest(route *gateway.Route, authorizer map[string]interface{}, body io.ReadCloser) (*events.APIGatewayProxyRequest, error) {
	queryParameters := r.URL.Query()
	pathVariables, _ := toolbox.ExtractURIParameters(route.URI, r.RequestURI)
	event := &events.APIGatewayProxyRequest{
		Resource:                        "",
		Path:                            r.RequestURI,
		HTTPMethod:                      r.Method,
		Headers:                         asHeaderMap(r.Header),
		MultiValueHeaders:               r.Header,
		QueryStringParameters:           asSingleValues(queryParameters),
		MultiValueQueryStringParameters: queryParameters,
		RequestContext: events.APIGatewayProxyRequestContext{
			Authorizer: authorizer,
		},

		PathParameters: pathVariables,
	}

	if body != nil {
		payload, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		if isASCII(payload) {
			event.Body = string(payload)
		} else {
			event.Body = base64.StdEncoding.EncodeToString(payload)
			event.IsBase64Encoded = true
		}
	}
	return event, nil
}

func asHeaderMap(header http.Header) map[string]string {
	result := map[string]string{}

	for aKey, values := range header {
		if len(values) == 0 {
			continue
		}

		result[aKey] = values[0]
	}

	return result
}

func asSingleValues(parameters url.Values) map[string]string {
	result := map[string]string{}
	for key, strings := range parameters {
		if len(strings) == 0 {
			continue
		}

		result[key] = strings[1]
	}

	return result
}

func (r *Request) AuthorizerRequest() *events.APIGatewayCustomAuthorizerRequest {
	return &events.APIGatewayCustomAuthorizerRequest{
		Type:               "",
		AuthorizationToken: r.Header.Get("Authorization"),
	}
}
