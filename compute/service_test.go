package compute

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/cloudless/compute/cluster"
	_ "github.com/viant/cloudless/compute/cluster/aws"
	_ "github.com/viant/cloudless/compute/cluster/gcp"
	"os"
	"testing"
)

func init() {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/vcarey/gbq.json")
}

func TestMatcher(t *testing.T) {
	var useCases = []struct {
		description string
		cluster     cluster.Cluster
	}{
		{
			description: "AWS test",
			cluster: cluster.Cluster{
				Discovery: cluster.Discovery{
					Api:     "AWS",
					Cluster: "Cluster1",
					Criteria: cluster.Criteria{
						Region: "us-west-2",
						Tags:   []string{"bidder"},
					},
				},
			},
		},
		{
			description: "GCP test",
			cluster: cluster.Cluster{
				Discovery: cluster.Discovery{
					Api:     "GCP",
					Cluster: "Cluster2",
					Criteria: cluster.Criteria{
						Project: "viant-e2e",
						Zone:    "us-east1-b",
						Tags:    []string{"aerospike"},
					},
				},
			},
		},
	}

	for _, useCase := range useCases {
		err := cluster.ClusterMatch(&useCase.cluster)
		assert.Nil(t, err, useCase.description)
		fmt.Printf("Cluster: %+v\n", useCase.cluster)
	}
}
