package lambda

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

type (
	FunctionConfig struct {
		lambda.FunctionConfiguration
		*Config
		CodeURI string
	}
	FunctionConfigOption func(c *FunctionConfig)
)

func (c *FunctionConfig) MergeFrom(config *lambda.FunctionConfiguration) {
	if c.Handler == nil {
		c.Handler = config.Handler
	}
	if c.MemorySize == nil {
		c.MemorySize = config.MemorySize
	}
	if c.Timeout == nil {
		c.Timeout = config.Timeout
	}
	if c.Runtime == nil {
		c.Runtime = config.Runtime
	}
	if c.Environment == nil {
		c.Environment = &lambda.EnvironmentResponse{}
	}
	if config.Environment != nil && len(config.Environment.Variables) > 0 {
		if len(c.Environment.Variables) == 0 {
			c.Environment.Variables = map[string]*string{}
		}
		for k, v := range config.Environment.Variables {
			if _, ok := c.Environment.Variables[k]; !ok {
				c.Environment.Variables[k] = v
			}
		}
	}

}

func (c *FunctionConfig) AddEnv(ctx context.Context, env *[]string, port int, xAmznTraceID string) error {
	if err := c.Config.AddEnv(ctx, env); err != nil {
		return err
	}
	*env = append(*env, "AWS_LAMBDA_FUNCTION_NAME="+*c.FunctionName)
	*env = append(*env, "AWS_LAMBDA_FUNCTION_VERSION="+*c.Version)
	*env = append(*env, "AWS_LAMBDA_FUNCTION_MEMORY_SIZE="+strconv.Itoa(int(*c.MemorySize)))
	awsCred, err := c.Cred(ctx)
	if err != nil {
		return err
	}
	*env = append(*env, "AWS_LAMBDA_LOG_GROUP_NAME="+path.Join(c.BaseLogLocation()+*c.FunctionName))
	*env = append(*env, "AWS_LAMBDA_LOG_STREAM_NAME="+logStreamName(*c.Version))
	*env = append(*env, "AWS_REGION="+awsCred.Region)
	*env = append(*env, "AWS_DEFAULT_REGION="+awsCred.Region)
	*env = append(*env, "_X_AMZN_TRACE_ID="+xAmznTraceID)
	*env = append(*env, "_LAMBDA_SERVER_PORT="+fmt.Sprintf("%v", port))
	if c.Environment != nil && len(c.Environment.Variables) > 0 {
		for k, v := range c.Environment.Variables {
			*env = append(*env, fmt.Sprintf("%v:%v", k, *v))
		}
	}
	return nil
}

func (c *FunctionConfig) Init(cfg *Config) {
	if c.Config == nil {
		c.Config = cfg
	}

	if c.FunctionName == nil {
		c.FunctionName = aws.String("test")
	}
	c.expandHandle()

	if c.Version == nil {
		c.Version = aws.String("latest")
	}
	if c.MemorySize == nil {
		c.MemorySize = aws.Int64(1536)
	}

	if c.Timeout == nil {
		c.Timeout = aws.Int64(300)
	}
	if c.Handler == nil {
		c.Handler = aws.String("handler")
	}

	awsCred, _ := c.Cred(context.Background())
	if c.FunctionArn == nil {
		arn := arn(awsCred.Region, strconv.Itoa(c.AccountID), *c.FunctionName)
		c.FunctionArn = &arn
	}
}

func (c *FunctionConfig) expandHandle() {
	if name := *c.Handler; name != "" {
		name = strings.ReplaceAll(name, "${GOOS}", runtime.GOOS)
		name = strings.ReplaceAll(name, "${GOARCH}", runtime.GOARCH)
		*c.Handler = name
	}
}

func arn(region string, accountID string, fnName string) string {
	nonDigit := regexp.MustCompile(`[^\d]`)
	return "arn:aws:lambda:" + region + ":" + nonDigit.ReplaceAllString(accountID, "") + ":function:" + fnName
}

func NewFunctionConfig(name, handler string, opts ...FunctionConfigOption) *FunctionConfig {
	ret := &FunctionConfig{}
	ret.FunctionName = &name
	ret.Handler = &handler
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
