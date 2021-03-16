package cluster

import (
	"fmt"
)

type Match func(criteria *Criteria) ([]Instance, error)

var registry = map[string]Match{}

func Register(api string, fn Match) {
	registry[api] = fn
}

type Service struct {
}

func (s *Service) Discover(discovery *Discovery) (*Cluster, error) {
	matchFn, ok := registry[discovery.Api]
	if !ok {
		return nil, fmt.Errorf(" invalid API: %s", discovery.Api)
	}
	instances, err := matchFn(&discovery.Criteria)
	if err != nil {
		return nil, err
	}

	// health check
	return &Cluster{
		Instances: s.filterByHealth(instances, discovery.HealthChecks),
	}, nil

}

func (s Service) filterByHealth(instances []Instance, checks []HealthCheck) []Instance {
	return instances
}
