package cluster

type Criteria struct {
	Region           string
	Zone             string
	AvailabilityZone string
	Tags             []string
	Labels           map[string]string
	Project          string
	URL              string
	Service          string
}
