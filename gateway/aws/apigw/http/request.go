package http

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/gateway"
	"github.com/viant/toolbox"
	"net/http"
)

type Request http.Request

//Request converts to http.Request
//apigw doesn't include the function name in the URI segments
func (r *Request) ProxyRequest(route *gateway.Route) *events.APIGatewayProxyRequest {
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
		PathParameters:                  pathVariables,
	}
}

func (r *Request) AuthorizerRequest() *events.APIGatewayCustomAuthorizerRequest {
	return nil
}
