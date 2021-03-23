# Cluster discovery

- [Motivation](#motivation)
- [Introduction](#introduction)
 - [Usage](#usage)
   * [AWS](#aws)
   * [Google cloud](#google-cloud)
   * [Consul](#consul)

## Motivation

Part of using a cloud infrastructure is need to discover/identify a collection of nodes, 
i.e. a cluster, that is used to implement certain functionality.  

## Introduction

Cluster discovery can be based on some attributes, e.g. EC2 tags in the AWS environment.  
Additionaly, the discovered nodes represented by their IP addresses can be filtered by
running health checks on the nodes. Currently, cluster discovery is implemented for Amazon, 
Google and Consul environments.


## Usage

To discover a cluster, one has to pass the following parameters:

- Cluster name (informational)
- API ("AWS", "GCP", "CONSUL"), a case insensitive string
- Criteria
- Health checks

Health checks can be implemented as
- HTTP check. Issues a GET request with predefined URL and checks for the 
  HTTP status code.
- Age check. Verifies whether the node is sufficiently "old".


#### AWS

```go
package main

import (
	"github.com/viant/cloudless/compute/cluster"
	_ "github.com/viant/cloudless/compute/cluster/aws"
	"time"
)

func main() (*cluster.Cluster, error) {
	// Discovery object contains criteria and health checks definitions
	discovery := &cluster.Discovery{
		Api:     "AWS",
		Cluster: "Cluster1",
		Criteria: cluster.Criteria{
			Region: "us-west-2",
			Tags:   []string{"mytag"},
		},
		HealthChecks: []cluster.HealthCheck{
			{
				URL:            "http://{IP}:8080/x/y/z",
				TimeoutMs:      1000,
				ExpectedStatus: 200,
				MinAge:         time.Minute * 10,
			},
		},
	}

	s := cluster.New()

	// discover cluster
	cluster, err := s.Discover(discovery)

}
```

#### Google cloud

```go
package compute

import (
	"github.com/viant/cloudless/compute/cluster"
	_ "github.com/viant/cloudless/compute/cluster/gcp"
	"time"
)

func main() {
	// Discovery object contains criteria and health checks definitions
	discovery := &cluster.Discovery{
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
	}

	s := cluster.New()

	// discover cluster
	cluster, err := s.Discover(discovery)

}
```

#### Consul

```go
package compute

import (
	"github.com/viant/cloudless/compute/cluster"
	_ "github.com/viant/cloudless/compute/cluster/gcp"
	"time"
)

func main() {
	// Discovery object contains criteria and health checks definitions
	discovery := &cluster.Discovery{
		Api:     "CONSUL",
		Cluster: "Cluster3",
		Criteria: cluster.Criteria{
			URL:     "consul.vianttech.com:8500",
			Service: "consul",
		},
	}

	s := cluster.New()

	// discover cluster
	cluster, err := s.Discover(discovery)

}
```

Consul can run health checks inside its own infrastructure and return only healthy
IP addresses.  However, it is possible to specify additional checks just the sa way as with AWS and GCP.
