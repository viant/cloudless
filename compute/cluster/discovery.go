package cluster

import "time"

type Discovery struct {
	Api     string
	Cluster string
	Criteria
	HealthChecks []HealthCheck
}

type HealthCheck struct {
	URL            string
	TimeoutMs      int
	ExpectedStatus int
	MaxRetries     int
	MinAge         time.Duration
	MinAgeSec      int
}

type Discoveries []Discovery
