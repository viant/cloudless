package aws

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var lbName = "my_load_balancer"
var region = "us-east-1"

func TestHostCount(t *testing.T) {
	count, err := HealthyHostCount(lbName, region)
	assert.Nil(t, err, "Getting healthy host count")
	fmt.Println("healthy host count:", count)
}
