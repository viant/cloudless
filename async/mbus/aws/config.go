package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	saws "github.com/viant/scy/auth/aws"
	"github.com/viant/scy/cred"
)

func awsConfig(ctx context.Context, awsCred *cred.Aws) (*aws.Config, error) {
	if awsCred == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	return saws.NewConfig(ctx, awsCred)
}
