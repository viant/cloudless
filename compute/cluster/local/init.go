package local

import "github.com/viant/cloudless/compute/cluster"

func init() {
	cluster.Register(API, Match)
}
