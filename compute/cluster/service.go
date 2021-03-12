package cluster

type Match func(criteria *Criteria) ([]Instance, error)

var registry = map[string]Match{}
