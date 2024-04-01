package gcp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	"os"
	"testing"
)

func init() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/user/xyz.json")
}

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		criteria    cluster.Criteria
	}{
		{
			description: "Unit test",
			criteria: cluster.Criteria{
				Project: "ppp",
				Zone:    "zzz",
				Tags:    []string{"aerospike"},
				Labels:  map[string]string{"service": "sss", "environment": "eee"},
			},
		},
	}

	for _, useCase := range useCases {
		ips, err := Match(&useCase.criteria)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("IPs: %+v\n", ips)
	}
}
