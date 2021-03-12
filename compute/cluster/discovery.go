package cluster

type Cluster struct {
	Discovery
	Instances []Instance
}

type Discovery struct {
	Api     string
	Cluster string
	Zone    string
	Criteria
	HealthChecks []HealthCheck
}

type HealthCheck struct {
	URL            string
	TimeoutMs      int
	ExpectedStatus int
	MaxRetries     int
}
