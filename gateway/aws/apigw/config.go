package apigw

import "github.com/aws/aws-sdk-go/aws"

type Config struct {
	Endpoint string
	Region   string
	AWS      *aws.Config
}
