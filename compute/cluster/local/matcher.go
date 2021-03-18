package local

import (
	"github.com/viant/cloudless/compute/cluster"
)

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	return []cluster.Instance{
		{
			Name:      "localhost",
			PrivateIP: "127.0.0.1",
		},
	}, nil
}
