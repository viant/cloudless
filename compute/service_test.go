package compute

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	_ "github.com/viant/cloudless/compute/cluster/aws"
	_ "github.com/viant/cloudless/compute/cluster/consul"
	_ "github.com/viant/cloudless/compute/cluster/gcp"
	"os"
	"testing"
	"time"
)

func init() {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/vcarey/gbq.json")
}

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		cluster.Discovery
	}{
		{
			description: "AWS test",
			Discovery: cluster.Discovery{
				Api:     "AWS",
				Cluster: "Cluster1",
				Criteria: cluster.Criteria{
					Region: "us-west-2",
					Tags:   []string{"bidder"},
				},
				HealthChecks: []cluster.HealthCheck{
					{
						URL:            "http://{IP}:8080/d/1x1.jpg",
						TimeoutMs:      1000,
						ExpectedStatus: 200,
						MinAge:         time.Minute * 10,
					},
				},
			},
		},
		{
			description: "GCP test",
			Discovery: cluster.Discovery{
				Api:     "GCP",
				Cluster: "Cluster2",
				Criteria: cluster.Criteria{
					Project: "viant-e2e",
					Zone:    "us-east1-b",
					Tags:    []string{"aerospike"},
				},
				HealthChecks: []cluster.HealthCheck{
					{
						MinAge: time.Duration(time.Minute * 10),
					},
				},
			},
		},
		{
			description: "CONSUL test",
			Discovery: cluster.Discovery{
				Api:     "CONSUL",
				Cluster: "Cluster3",
				Criteria: cluster.Criteria{
					URL:     "consul.vianttech.com:8500",
					Service: "consul",
				},
			},
		},
	}

	s := cluster.New()
	for _, useCase := range useCases[0:1] {
		cluster, err := s.Discover(&useCase.Discovery)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("Cluster: %+v\n", cluster)
	}
}
