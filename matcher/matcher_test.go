package matcher

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type route struct {
	uri    string
	method string
}

func (r *route) URI() string {
	return r.uri
}

func (r *route) Namespaces() []string {
	return []string{r.method}
}

func TestMatcher(t *testing.T) {
	testCases := []struct {
		description  string
		routes       []Matchable
		route        string
		matchedRoute string
		expectError  bool
		method       string
	}{
		{
			description: "basic match",
			routes:      []Matchable{&route{uri: "/events", method: http.MethodGet}},
			route:       "/events",
			method:      http.MethodGet,
		},
		{
			description: "multiple routes",
			routes:      []Matchable{&route{uri: "/events", method: http.MethodGet}, &route{uri: "/foos", method: http.MethodGet}},
			route:       "/foos",
			method:      http.MethodGet,
		},
		{
			description: "nested route",
			routes:      []Matchable{&route{uri: "/events/seg1/seg2/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2", method: http.MethodGet}},
			route:       "/events/seg1/seg2/seg3",
			method:      http.MethodGet,
		},
		{
			description: "nested route",
			routes:      []Matchable{&route{uri: "/events/seg1/seg2/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2", method: http.MethodGet}},
			route:       "/events/seg1/seg2",
			method:      http.MethodGet,
		},
		{
			description:  "wildcard route",
			routes:       []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2", method: http.MethodGet}},
			route:        "/events/seg1/1/seg3",
			matchedRoute: "/events/seg1/{segID}/seg3",
			method:       http.MethodGet,
		},
		{
			description:  "post method",
			routes:       []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2", method: http.MethodGet}},
			route:        "/events/seg1/1/seg3",
			matchedRoute: "/events/seg1/{segID}/seg3",
			method:       http.MethodPost,
			expectError:  true,
		},
		{
			description: "exact precedence",
			routes:      []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2/seg4", method: http.MethodGet}},
			route:       "/events/seg1/seg2/seg4",
			method:      http.MethodGet,
		},
		{
			description: "icnorrect route",
			routes:      []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2/seg4", method: http.MethodGet}},
			route:       "//",
			expectError: true,
			method:      http.MethodGet,
		},
		{
			description:  "query param",
			routes:       []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2/seg4", method: http.MethodGet}},
			route:        "/events/seg1/seg2/seg4?abc=true",
			matchedRoute: "/events/seg1/seg2/seg4",
			method:       http.MethodGet,
		},
		{
			description: "includes route",
			routes:      []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2/seg4", method: http.MethodGet}},
			route:       "/events/seg1/seg2/seg4/abc/def/ghi/jkl?abc=true",
			method:      http.MethodGet,
			expectError: true,
		},
		{
			description: "empty path",
			routes:      []Matchable{&route{uri: "/events/seg1/{segID}/seg3", method: http.MethodGet}, &route{uri: "/events/seg1/seg2/seg4", method: http.MethodGet}},
			route:       "",
			method:      http.MethodGet,
			expectError: true,
		},
		{
			description: "v1/api/meta/view/",
			routes:      []Matchable{&route{uri: "v1/api/meta/view/"}},
			route:       `v1/api/meta/view/`,
		},
		{
			description: "/v1/api/meta/view",
			routes:      []Matchable{&route{uri: "//v1/api/meta/view"}},
			route:       `//v1/api/meta/view`,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		matcher := NewMatcher(testCase.routes)
		match, err := matcher.MatchOne(testCase.method, testCase.route)
		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

		matchedURI := testCase.matchedRoute
		if matchedURI == "" {
			matchedURI = testCase.route
		}

		assert.Nil(t, err, testCase.description)
		assert.Equal(t, matchedURI, match.URI(), testCase.description)
	}
}
