package sam

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/viant/cloudless/container/aws/lambda"
	"os"
)

func (t *Template) LambdaConfig() (*lambda.Config, error) {
	baseDir, _ := os.Getwd()
	cfg := &lambda.Config{}
	globalFunc := t.Globals.Function
	for name, res := range t.Resources {
		switch res.Type {
		case "AWS::Serverless::Function":
			fnConfig := &lambda.FunctionConfig{}
			fnConfig.FunctionName = aws.String(name)
			fnConfig.CodeURI = res.Properties.CodeURL(baseDir)
			fnConfig.MergeFrom(&globalFunc.FunctionConfiguration)
			cfg.Functions = append(cfg.Functions, fnConfig)
		}
	}

	return cfg, nil
}
