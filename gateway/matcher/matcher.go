package matcher

import (
	"fmt"
	"strings"
)

type (
	Matchable interface {
		URI() string
		Namespaces() []string
	}

	Matcher struct {
		Nodes       []*Node
		Matchables  []Matchable
		methodIndex map[string]int
	}

	Node struct {
		Matched         []int
		ExactChildren   []*Node
		WildcardMatcher *Node
		StarMatcher     *Node // Handles "*" wildcards that match multiple segments
		urlIndex        map[string]int
		positionIndex   map[int]bool
		suffix          bool
	}

	NodeMatch struct {
		URL  string
		Node *Node
	}
)

func (n *Node) Add(routeIndex int, uri string) {
	if uri == "" {
		if !n.positionIndex[routeIndex] {
			n.Matched = append(n.Matched, routeIndex)
			n.positionIndex[routeIndex] = true
		}
		return
	}

	segment, remaining := extractSegment(uri)
	var child *Node

	if segment == "*" {
		// "*" wildcard that matches everything below this path
		child = n.getStarMatcher()
		child.suffix = true
		// For wildcard star, we add the route at this level
		if !child.positionIndex[routeIndex] {
			child.Matched = append(child.Matched, routeIndex)
			child.positionIndex[routeIndex] = true
		}
		return
	} else if len(segment) > 0 && segment[0] == '{' {
		// Parameter wildcard like {id}
		child = n.getWildcardMatcher()
	} else {
		// Normal path segment
		child = n.getChildOrCreate(segment)
	}

	child.Add(routeIndex, remaining)
}

func (n *Node) getChildOrCreate(segment string) *Node {
	if childIndex, ok := n.urlIndex[segment]; ok {
		return n.ExactChildren[childIndex]
	}

	n.urlIndex[segment] = len(n.ExactChildren)
	child := NewNode()
	n.ExactChildren = append(n.ExactChildren, child)
	return child
}

func NewNode() *Node {
	return &Node{
		urlIndex:      map[string]int{},
		positionIndex: map[int]bool{},
	}
}

func (n *Node) Match(method, route string, exact bool, dest *[]*Node) {
	if route == "" {
		*dest = append(*dest, n)
		return
	}

	segment, path := extractSegment(route)

	// Check for exact match first - prioritize exact matches over wildcards
	if index, ok := n.urlIndex[segment]; ok {
		node := n.ExactChildren[index]
		if path == "" {
			node.Match(method, path, exact, dest)
			return
		}
		node.Match(method, path, exact, dest)
		return
	}

	// Check for parameter wildcard
	if n.WildcardMatcher != nil {
		n.WildcardMatcher.Match(method, path, exact, dest)
		return
	}

	// Check if we have a star matcher that matches everything
	if n.StarMatcher != nil {
		*dest = append(*dest, n.StarMatcher)
		return
	}

	if !exact {
		*dest = append(*dest, n)
	}
}

func (n *Node) nextMatcher(segment string) (*Node, bool) {
	// Check for exact match
	index, ok := n.urlIndex[segment]
	if ok {
		return n.ExactChildren[index], true
	}

	// Check for wildcard match
	if n.WildcardMatcher != nil {
		return n.WildcardMatcher, true
	}

	return nil, false
}

func (n *Node) getWildcardMatcher() *Node {
	if n.WildcardMatcher != nil {
		return n.WildcardMatcher
	}

	n.WildcardMatcher = NewNode()
	return n.WildcardMatcher
}

func (n *Node) getStarMatcher() *Node {
	if n.StarMatcher != nil {
		return n.StarMatcher
	}

	n.StarMatcher = NewNode()
	return n.StarMatcher
}

func extractSegment(uri string) (string, string) {
	uri = AsRelative(uri)

	if segIndex := strings.IndexByte(uri, '/'); segIndex != -1 {
		return uri[:segIndex], uri[segIndex+1:]
	}

	return uri, ""
}

func (m *Matcher) match(method string, route string, exact bool) ([]*Node, error) {
	relative := AsRelative(route)

	methodMatcher, ok := m.getMethodMatcher(method)
	if !ok {
		return nil, m.unmatchedRouteErr(route)
	}

	var matched []*Node
	methodMatcher.Match(method, relative, exact, &matched)
	if len(matched) == 0 {
		return nil, fmt.Errorf("couldn't match URI %v", route)
	}

	return matched, nil
}

func (m *Matcher) unmatchedRouteErr(route string) error {
	return fmt.Errorf("couldn't match URI %v", route)
}

func (m *Matcher) getMethodMatcher(method string) (*Node, bool) {
	index, ok := m.methodIndex[method]
	if !ok {
		return nil, false
	}

	return m.Nodes[index], true
}

func (m *Matcher) init() {
	m.methodIndex = map[string]int{}
	for i, route := range m.Matchables {
		uri := AsRelative(route.URI())

		namespaces := route.Namespaces()
		for _, namespace := range namespaces {
			node := m.getOrCreateMatcher(namespace)
			node.Add(i, uri)

			allUriNodes := m.getOrCreateMatcher("")
			allUriNodes.Add(i, uri)
		}
	}
}

func (m *Matcher) getOrCreateMatcher(method string) *Node {
	matcher, ok := m.getMethodMatcher(method)
	if ok {
		return matcher
	}

	node := NewNode()
	m.methodIndex[method] = len(m.Nodes)
	m.Nodes = append(m.Nodes, node)
	return node
}

func (m *Matcher) MatchPrefix(method string, uriPath string) ([]Matchable, error) {
	allMatch, err := m.match(method, uriPath, false)
	if err != nil {
		return nil, err
	}

	return m.flatten(allMatch), nil
}

func (m *Matcher) firstMatched(match *Node) Matchable {
	return m.Matchables[match.Matched[0]]
}

func (m *Matcher) flatten(match []*Node) []Matchable {
	totalMatched := 0
	for _, node := range match {
		totalMatched += len(node.Matched)
	}

	matchables := make([]Matchable, 0, totalMatched)
	for _, node := range match {
		for _, i := range node.Matched {
			matchables = append(matchables, m.Matchables[i])
		}
	}

	return matchables
}

func AsRelative(route string) string {
	if len(route) == 0 {
		return route
	}

	var i int
	for ; i < len(route) && route[i] == '/'; i++ {
	}

	if i >= len(route)-1 {
		return route
	}

	route = route[i:]

	if paramsStartIndex := strings.IndexByte(route, '?'); paramsStartIndex != -1 {
		route = route[:paramsStartIndex]
	}

	return route
}

func (m *Matcher) MatchOne(namespace, URI string) (Matchable, error) {
	match, err := m.match(namespace, URI, true)
	if err != nil {
		return nil, err
	}

	if len(match) == 0 || (len(match) == 1 && len(match[0].Matched) == 0) {
		return nil, m.unmatchedRouteErr(URI)
	}

	// Prioritize exact matches
	var exactMatch *Node
	var otherMatches []*Node

	for _, node := range match {
		if !node.suffix && len(node.Matched) > 0 {
			exactMatch = node
			break
		}
		if len(node.Matched) > 0 {
			otherMatches = append(otherMatches, node)
		}
	}

	// Use exact match if found
	if exactMatch != nil && len(exactMatch.Matched) > 0 {
		return m.Matchables[exactMatch.Matched[0]], nil
	}

	// Otherwise use the first match with routes
	for _, node := range otherMatches {
		if len(node.Matched) > 0 {
			return m.Matchables[node.Matched[0]], nil
		}
	}

	return nil, fmt.Errorf("matched more than one route for %v", URI)
}

func (m *Matcher) MatchAll(namespace, URI string) []Matchable {
	match, err := m.match(namespace, URI, true)
	if err != nil {
		return nil
	}

	matched := make([]Matchable, 0, len(match))
	for _, node := range match {
		for _, index := range node.Matched {
			matched = append(matched, m.Matchables[index])
		}
	}

	return matched
}

func NewMatcher(matchables []Matchable) *Matcher {
	m := &Matcher{
		Matchables: matchables,
	}

	m.init()
	return m
}
