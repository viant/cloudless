package gcp

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"time"
)

//Config represent Pub/Sub subscriber config
type Config struct {
	processor.Config
	ProjectID    string
	Subscription string
	BatchSize    int           // message batch to be processed
	Concurrency  int           // concurrency
	MaxExtension time.Duration // ack deadline time
}

//Init initialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	c.Config.Init(ctx, fs)
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.Concurrency == 0 {
		c.Concurrency = 20
	}
	if c.MaxExtension == 0 {
		c.MaxExtension = 60 * time.Minute
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
