package lb

type NodeCountRequest struct {
	Region            string
	LoadBalancerNames []string
}

type NodeCountResponse struct {
	Region string
	Count  int
}

type NodeCountResponses []*NodeCountResponse

func (c NodeCountResponses) NodeCount() int {
	nodeCount := 0
	if len(c) == 0 {
		return 0
	}
	for _, item := range c {
		nodeCount += item.Count
	}
	return nodeCount
}
