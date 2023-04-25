package lb

type NodeCountRequest struct {
	Region            string
	LoadBalancerNames []string
}

type NodeCountResponse struct {
	Region string
	Count  int
}
