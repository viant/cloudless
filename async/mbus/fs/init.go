package mem

import "github.com/viant/cloudless/async/mbus"

func init() {
	mbus.Register("fs", New())
	mbus.RegisterNotifier("fs", NewNotifier())
}
