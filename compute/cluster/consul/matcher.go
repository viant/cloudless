package consul

import (
	"github.com/hashicorp/consul/api"
	"github.com/viant/cloudless/compute/cluster"
)

const okStatus = "passing"

func init() {
	cluster.Register("CONSUL", Match)
}

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {

	cfg := api.DefaultConfig()
	cfg.Address = criteria.ConsulURL
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	cat := client.Catalog()
	consulNodes, _, err := cat.Service(criteria.ConsulService, "", nil)
	if err != nil {
		return nil, err
	}

	instances := make([]cluster.Instance, 0)
	for _, node := range consulNodes {
		if node.Checks.AggregatedStatus() == okStatus {
			instances = append(instances, cluster.Instance{
				Name:      node.Node,
				PrivateIP: node.Address,
				//StartTime: tm,
			})
		}
	}

	return instances, nil
}
