package aws

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
)

//Config represent sqs subscriber config
type Config struct {
	processor.Config
	QueueName         string
	BatchSize         int
	WaitTimeSeconds   int
	VisibilityTimeout int
}

//Initinitialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	c.Config.Init(ctx, fs)
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	if c.WaitTimeSeconds == 0 {
		c.WaitTimeSeconds = 20
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
	if c.QueueName == "" {
		return fmt.Errorf("queue name is empty")
	}
	return nil
}
