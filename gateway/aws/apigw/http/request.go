package http

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/gateway"
	"net/http"
)

type Request http.Request

//Request converts to http.Request
//apigw doesn't include the function name in the URI segments
func (r *Request) Request(route *gateway.Route) *events.APIGatewayProxyRequest {

	return nil
}
