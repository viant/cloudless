package cluster

type Criteria struct {
	Region        string
	Zone          string
	Tags          []string
	Project       string
	ConsulURL     string
	ConsulService string
}
