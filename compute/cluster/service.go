package cluster

import (
	"fmt"
	"time"
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
		Discovery: *discovery,
		Instances: s.filterByHealth(instances, discovery.HealthChecks),
	}, nil

}

func (s Service) filterByHealth(instances []Instance, checks []HealthCheck) []Instance {
	if len(checks) > 0 {
		return s.filterByAge(instances, checks[0].MinAge)
	}
	return instances
}

func (s *Service) filterByAge(instances []Instance, age time.Duration) []Instance {
	n := 0
	for _, inst := range instances {
		if time.Now().Sub(inst.StartTime) >= age {
			instances[n] = inst
			n++
		}
	}
	return instances[:n]
}
