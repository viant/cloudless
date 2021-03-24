package gcp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	"os"
	"testing"
)

func init() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/user/gbq.json")
}

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		criteria    cluster.Criteria
	}{
		{
			description: "Unit test",
			criteria: cluster.Criteria{
				Project: "xyz",
				Zone:    "us-east1-b",
				Tags:    []string{"aerospike"},
			},
		},
	}

	for _, useCase := range useCases {
		ips, err := Match(&useCase.criteria)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("IPs: %+v\n", ips)
	}
}
