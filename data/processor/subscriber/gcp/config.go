package gcp

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
)

//Config represent Pub/Sub subscriber config
type Config struct {
	processor.Config
	ProjectID          string
	Subscription       string
	BatchSize          int // message batch to be processed
	MessageConcurrency int // concurrency
	VisibilityTimeout  int // ack deadline time in sec
}

//Init initialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	c.Config.Init(ctx, fs)
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.MessageConcurrency == 0 {
		c.MessageConcurrency = 20
	}
	if c.VisibilityTimeout == 0 {
		tmp := 43200 // 12 hours allowed max
		if 2*c.MaxExecTimeMs*1000 < tmp {
			tmp = 2 * c.MaxExecTimeMs * 1000
		}
		c.VisibilityTimeout = tmp
	}
	return nil
}

//Validate validates config
func (c *Config) Validate() error {
	if c.Subscription == "" {
		return fmt.Errorf("subscription were empty")
	}
	return nil
}
