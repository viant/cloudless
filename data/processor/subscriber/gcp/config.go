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
	ProjectID    string
	Subscription string
	BatchSize    int
	UseSubscriptionConcurrency bool
}

//Initinitialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	c.Config.Init(ctx, fs)
	if c.BatchSize == 0 {
		c.BatchSize = 10
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
