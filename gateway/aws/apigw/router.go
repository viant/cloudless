package apigw

import (
	"github.com/viant/cloudless/gateway"
	"github.com/viant/cloudless/gateway/matcher"
	"net/http"
)

type (
	Router struct {
		matcher *matcher.Matcher
	}

	matchable struct {
		route *gateway.Route
	}
)

func NewRouter(routes []*gateway.Route) *Router {
	matchables := make([]matcher.Matchable, 0, len(routes))
	for _, route := range routes {
		matchables = append(matchables, &matchable{
			route: route,
		})
	}

	aMatcher := matcher.NewMatcher(matchables)
	return &Router{matcher: aMatcher}
}
func (c *Router) FindRoute(request *http.Request) (*gateway.Route, error) {
	aRoute, err := c.matcher.MatchOne(request.Method, request.RequestURI)
	if err != nil {
		return nil, err
	}

	route := aRoute.(*matchable)
	return route.route, nil
}

func (m *matchable) URI() string {
	return m.route.URI
}

func (m *matchable) Namespaces() []string {
	return []string{m.route.HTTPMethod}
}
