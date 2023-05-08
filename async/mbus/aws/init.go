package aws

import "github.com/viant/cloudless/async/mbus"

func init() {
	mbus.Register("aws", New())
}
