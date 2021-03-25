package cluster

type Criteria struct {
	Region           string
	Zone             string
	AvailabilityZone string
	Tags             []string
	Project          string
	URL              string
	Service          string
}
