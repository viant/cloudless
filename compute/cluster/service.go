package cluster

import (
	"fmt"
)

type Match func(criteria *Criteria) ([]Instance, error)

var registry = map[string]Match{}

func Register(api string, fn Match) {
	registry[api] = fn
}

func ClusterMatch(c *Cluster) error {
	matchFn, ok := registry[c.Api]
	if !ok {
		return fmt.Errorf(" invalid API: %s", c.Api)
	}
	instances, err := matchFn(&c.Criteria)
	if err != nil {
		return err
	}
	c.Instances = instances
	return nil
}
