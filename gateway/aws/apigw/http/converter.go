package http

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/cloudless/gateway"
	"github.com/viant/cloudless/matcher"
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
)

type (
	Converter struct {
		matcher *matcher.Matcher
	}

	matchable struct {
		route *gateway.Route
	}
)

func NewConverter(routes []*gateway.Route) *Converter {
	matchables := make([]matcher.Matchable, 0, len(routes))
	for _, route := range routes {
		matchables = append(matchables, &matchable{
			route: route,
		})
	}

	aMatcher := matcher.NewMatcher(matchables)
	return &Converter{matcher: aMatcher}
}

func (c *Converter) Convert(request *http.Request) (*events.APIGatewayProxyRequest, error) {
	aRoute, err := c.matcher.MatchOne(request.Method, request.RequestURI)
	if err != nil {
		return nil, err
	}

	route := aRoute.(*matchable)

	queryParameters := request.URL.Query()
	pathVariables, _ := toolbox.ExtractURIParameters(route.route.URI, request.RequestURI)

	return &events.APIGatewayProxyRequest{
		Resource:                        "",
		Path:                            request.RequestURI,
		HTTPMethod:                      request.Method,
		Headers:                         asHeaderMap(request.Header),
		MultiValueHeaders:               request.Header,
		QueryStringParameters:           asSingleValues(queryParameters),
		MultiValueQueryStringParameters: queryParameters,
		PathParameters:                  pathVariables,
	}, nil
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

func (m *matchable) URI() string {
	return m.route.URI
}

func (m *matchable) Namespaces() []string {
	return []string{m.route.HTTPMethod}
}
