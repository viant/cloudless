package cluster

import "time"

type Instance struct {
	Name      string
	PrivateIP string
	StartTime time.Time
}
