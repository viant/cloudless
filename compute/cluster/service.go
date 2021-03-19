package cluster

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Match func(criteria *Criteria) ([]Instance, error)

var registry = map[string]Match{}

func Register(api string, fn Match) {
	registry[api] = fn
}

type Service struct {
}

func New() *Service {
	return &Service{}
}

func (s *Service) Discover(discovery *Discovery) (*Cluster, error) {
	matchFn, ok := registry[strings.ToUpper(discovery.Api)]
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
	for _, hc := range checks {
		if hc.MinAge > 0 {
			instances = s.filterByAge(instances, hc.MinAge)
		}
		if hc.URL != "" {
			instances = s.filterByHttp(instances, hc)
		}
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

func (s *Service) filterByHttp(instances []Instance, hc HealthCheck) []Instance {
	ipState := make([]chan bool, len(instances))
	for i, _ := range ipState {
		ipState[i] = make(chan bool)
	}

	for i, inst := range instances {
		go checkIP(inst.PrivateIP, hc, ipState[i])
	}

	n := 0
	for i, inst := range instances {
		if <-ipState[i] {
			instances[n] = inst
			n++
		}
	}
	fmt.Printf("instances: %v -> %v\n", len(instances), n)
	return instances[:n]
}

func checkIP(ip string, hc HealthCheck, result chan bool) {
	httpClient := &http.Client{
		Timeout: time.Millisecond * time.Duration(hc.TimeoutMs),
	}
	resp, err := httpClient.Get(strings.Replace(hc.URL, ipVar, ip, 1))
	if err != nil {
		result <- false
		return
	}
	resp.Body.Close()
	result <- resp.StatusCode == 200
}
