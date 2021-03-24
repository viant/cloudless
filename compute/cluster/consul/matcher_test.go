package consul

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	"testing"
)

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		criteria    cluster.Criteria
	}{
		{
			description: "Unit test",
			criteria: cluster.Criteria{
				URL:     "consul.company.com:8500",
				Service: "consul",
			},
		},
	}

	for _, useCase := range useCases {
		ips, err := Match(&useCase.criteria)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("IPs: %+v\n", ips)
	}
}
