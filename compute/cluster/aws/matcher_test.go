package aws

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	"os"
	"testing"
)

func init() {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
}

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		criteria    cluster.Criteria
	}{
		{
			description: "Unit test",
			criteria: cluster.Criteria{
				Region: "us-west-2",
				Tags:   []string{"service:bidder", "environment:production"},
			},
		},
	}

	for _, useCase := range useCases {
		ips, err := Match(&useCase.criteria)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("IPs: %+v\n", ips)
	}
}
