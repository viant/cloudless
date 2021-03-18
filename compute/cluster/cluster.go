package cluster

type Cluster struct {
	Discovery
	Instances []Instance
}

type Clusters []Cluster
