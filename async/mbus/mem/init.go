package mem

import "github.com/viant/cloudless/async/mbus"

func init() {
	mbus.Register("mem", New())
}
