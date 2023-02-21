package lambda

import (
	"context"
	"fmt"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
)

type (
	Config struct {
		Debug        *Debug
		AccountID    int
		FuncLocation string
		LogLocation  string
		Secret       *scy.Resource
		AccessSecret *scy.Resource
		cred         *cred.Aws
		accessCred   *cred.Aws
		Functions    []*FunctionConfig
		Port         int
		functionPort int32
		lock         sync.Mutex
	}

	ConfigOption func(c *Config)

	Debug struct {
		Enabled bool
		Delve   Delve
	}

	Delve struct {
		Port     int
		Location string
		API      string
	}
)

func (c *Config) Lookup(fnName string) *FunctionConfig {
	for _, candidate := range c.Functions {
		if *candidate.FunctionName == fnName {
			return candidate
		}
	}
	return nil
}

func (c *Config) AddEnv(ctx context.Context, env *[]string) error {
	awsCred, err := c.Cred(ctx)
	if err != nil {
		return err
	}
	*env = append(*env, "AWS_ACCESS_KEY="+awsCred.Secret)
	*env = append(*env, "AWS_ACCESS_KEY_ID="+awsCred.Key)
	if session := awsCred.Session; session != nil && awsCred.Token != "" {
		*env = append(*env, "AWS_SECURITY_TOKEN="+awsCred.Token)
		*env = append(*env, "AWS_SECURITY_TOKEN="+session.Name)
	}
	accessCred, err := c.AccessCred(ctx)
	if err != nil {
		return err
	}
	*env = append(*env, "AWS_ACCESS_KEY="+accessCred.Secret)
	*env = append(*env, "AWS_ACCESS_KEY_ID="+accessCred.Key)
	return nil
}

func (c *Config) Cred(ctx context.Context) (*cred.Aws, error) {
	if c.cred != nil {
		return c.cred, nil
	}
	awsCred, err := c.loadAwsCred(ctx, c.Secret)
	if err != nil {
		return nil, err
	}
	c.cred = awsCred
	return awsCred, nil
}

func (c *Config) AccessCred(ctx context.Context) (*cred.Aws, error) {
	if c.accessCred != nil {
		return c.cred, nil
	}
	awsCred, err := c.loadAwsCred(ctx, c.AccessSecret)
	if err != nil {
		return nil, err
	}
	c.cred = awsCred
	return awsCred, nil
}

func (c *Config) loadAwsCred(ctx context.Context, resource *scy.Resource) (*cred.Aws, error) {
	if c.Secret == nil {
		awsCred := &cred.Aws{
			Key:    os.Getenv("AWS_ACCESS_KEY_ID"),
			Secret: os.Getenv("AWS_SECRET_KEY"),
			Region: os.Getenv("AWS_REGION"),
		}
		if awsCred.Secret == "" {
			awsCred.Secret = "dummy"
			awsCred.Key = "dummy"
		}
		return awsCred, nil
	}
	srv := scy.New()
	c.Secret.SetTarget(reflect.TypeOf(&cred.Aws{}))
	secret, err := srv.Load(ctx, resource)
	if err != nil {
		return nil, err
	}
	awsCred, ok := secret.Target.(*cred.Aws)
	if !ok {
		return nil, fmt.Errorf("invalid awsCred type: expected :%T, but had: %T", awsCred, secret.Target)
	}
	return awsCred, nil
}

func (c *Config) BaseHandlerLocation() string {
	return path.Join(c.FuncLocation, "task")
}

func (c *Config) BaseLogLocation() string {
	return path.Join(c.LogLocation, "log")
}

func (c *Config) nextPort() int {
	if c.functionPort == 0 {
		c.functionPort = 5432
	}
	for port := int(c.functionPort); port < int(c.functionPort)+10000; port++ {
		if isPortAvailable(port) {
			atomic.StoreInt32(&c.functionPort, int32(port+1))
			return int(port)
		}
	}
	return 0
}

func (c *Config) Init() {
	for _, fn := range c.Functions {
		fn.Init(c)
	}
	if c.Port == 0 {
		c.Port = 9001
	}
}

func WithFunction(config *FunctionConfig) ConfigOption {
	return func(c *Config) {
		c.Functions = append(c.Functions, config)
	}
}

//NewConfig creates a config
func NewConfig(port int, fnLocation, logLocation string, opts ...ConfigOption) *Config {
	ret := &Config{
		Port:         port,
		FuncLocation: fnLocation,
		LogLocation:  logLocation,
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
