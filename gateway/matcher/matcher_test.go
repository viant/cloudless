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
			description: "incorrect route",
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
		{
			description:  "wildcard star match - exact path",
			routes:       []Matchable{&route{uri: "/api/v1/foo/*", method: http.MethodGet}},
			route:        "/api/v1/foo/bar",
			matchedRoute: "/api/v1/foo/*",
			method:       http.MethodGet,
		},
		{
			description:  "wildcard star match - deeper path",
			routes:       []Matchable{&route{uri: "/api/v1/foo/*", method: http.MethodGet}},
			route:        "/api/v1/foo/bar/baz",
			matchedRoute: "/api/v1/foo/*",
			method:       http.MethodGet,
		},
		{
			description:  "wildcard star match - exact precedence",
			routes:       []Matchable{&route{uri: "/api/v1/foo/bar", method: http.MethodGet}, &route{uri: "/api/v1/foo/*", method: http.MethodGet}},
			route:        "/api/v1/foo/bar",
			matchedRoute: "/api/v1/foo/bar",
			method:       http.MethodGet,
		},
		{
			description:  "wildcard star match - multiple wildcards",
			routes:       []Matchable{&route{uri: "/api/v1/foo/*", method: http.MethodGet}, &route{uri: "/api/v1/bar/*", method: http.MethodGet}},
			route:        "/api/v1/foo/xxxxx?12321",
			matchedRoute: "/api/v1/foo/*",
			method:       http.MethodGet,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			matcher := NewMatcher(testCase.routes)
			match, err := matcher.MatchOne(testCase.method, testCase.route)
			if testCase.expectError {
				assert.NotNil(t, err, testCase.description)
				return
			}

			matchedURI := testCase.matchedRoute
			if matchedURI == "" {
				matchedURI = testCase.route
			}

			assert.Nil(t, err, testCase.description)
			assert.Equal(t, matchedURI, match.URI(), testCase.description)
		})
	}
}

func TestWildcardStarMatcher(t *testing.T) {
	routes := []Matchable{
		&route{uri: "/api/v1/foo/*", method: http.MethodGet},
		&route{uri: "/api/v1/foo/specific", method: http.MethodGet},
		&route{uri: "/api/v1/bar/{id}", method: http.MethodGet},
	}

	matcher := NewMatcher(routes)

	testCases := []struct {
		path        string
		expectedURI string
		shouldMatch bool
		method      string
		description string
	}{
		{
			path:        "/api/v1/foo/anything",
			expectedURI: "/api/v1/foo/*",
			shouldMatch: true,
			method:      http.MethodGet,
			description: "basic wildcard match",
		},
		{
			path:        "/api/v1/foo/specific",
			expectedURI: "/api/v1/foo/specific",
			shouldMatch: true,
			method:      http.MethodGet,
			description: "exact match should take precedence",
		},
		{
			path:        "/api/v1/foo/nested/path/with/many/segments",
			expectedURI: "/api/v1/foo/*",
			shouldMatch: true,
			method:      http.MethodGet,
			description: "deeply nested path should match wildcard",
		},
		{
			path:        "/api/v1/other/path",
			shouldMatch: false,
			method:      http.MethodGet,
			description: "unrelated path shouldn't match",
		},
		{
			path:        "/api/v1/bar/123",
			expectedURI: "/api/v1/bar/{id}",
			shouldMatch: true,
			method:      http.MethodGet,
			description: "parameter wildcard should still work",
		},
		{
			path:        "/api/v1/foo/anything",
			shouldMatch: false,
			method:      http.MethodPost,
			description: "method mismatch should fail",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			match, err := matcher.MatchOne(tc.method, tc.path)

			if !tc.shouldMatch {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedURI, match.URI())
		})
	}
}
