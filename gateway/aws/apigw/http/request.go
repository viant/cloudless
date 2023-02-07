package http

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/gateway"
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
)

type Request http.Request

//Request converts to http.Request
//apigw doesn't include the function name in the URI segments
func (r *Request) ProxyRequest(route *gateway.Route, authorizer map[string]interface{}) *events.APIGatewayProxyRequest {
	queryParameters := r.URL.Query()
	pathVariables, _ := toolbox.ExtractURIParameters(route.URI, r.RequestURI)
	return &events.APIGatewayProxyRequest{
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
