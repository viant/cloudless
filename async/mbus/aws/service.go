package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"github.com/viant/toolbox"
	"sync"
)

type Service struct {
	resources map[string]*mbus.Resource
	sync.Mutex
	config *aws.Config
}

func (s *Service) Push(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	return s.sendMessage(ctx, dest, message)
}

func (s *Service) sendMessage(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	queueURL, err := s.getQueueURL(ctx, dest)
	if err != nil {
		return nil, err
	}
	input := &sqs.SendMessageInput{
		DelaySeconds: 1,
		QueueUrl:     &queueURL,
	}
	if len(message.Attributes) > 0 {
		input.MessageAttributes = make(map[string]types.MessageAttributeValue)
		putSqsMessageAttributes(message.Attributes, input.MessageAttributes)
	}
	body, err := message.Payload()
	if err != nil {
		return nil, err
	}

	input.MessageBody = aws.String(string(body))
	client, err := s.client(ctx, dest)
	if err != nil {
		return nil, err
	}
	result, err := client.SendMessage(ctx, input)
	if err != nil {
		return nil, err
	}
	confirmation := &mbus.Confirmation{
		MessageID: *result.MessageId,
	}
	return confirmation, nil
}

func (s *Service) getQueueURL(ctx context.Context, resource *mbus.Resource) (string, error) {
	client, err := s.client(ctx, resource)
	if err != nil {
		return "", err
	}
	result, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(resource.Name),
	})
	if err != nil {
		return "", fmt.Errorf("failed to lookup queue URL %v", resource.Name)
	}
	return *result.QueueUrl, nil
}

//queue returns queue
func (s *Service) client(ctx context.Context, dest *mbus.Resource) (*sqs.Client, error) {
	dest.Lock()
	if dest.Client != nil {
		if ret, ok := dest.Client.(*sqs.Client); ok {
			return ret, nil
		}
	}
	defer dest.Unlock()
	cfg, err := s.awsConfig(ctx, dest)
	if err != nil {
		return nil, err
	}
	if dest.Region != "" {
		cfg.Region = dest.Region
	}
	client := sqs.NewFromConfig(*cfg)
	dest.Client = client
	return client, nil
}

func (s *Service) awsConfig(ctx context.Context, dest *mbus.Resource) (*aws.Config, error) {
	var awsCred *cred.Aws
	var err error
	if dest.Credentials != nil {
		awsCred, err = s.loadAwsCredentials(ctx, dest.Credentials)
		if err != nil {
			return nil, err
		}

	}
	return awsConfig(ctx, awsCred)
}

func (s *Service) loadAwsCredentials(ctx context.Context, resource *scy.Resource) (*cred.Aws, error) {
	srv := scy.New()
	secret, err := srv.Load(ctx, scy.NewResource(&aws.Config{}, resource.URL, resource.Key))
	if err != nil {
		return nil, err
	}
	ret, ok := secret.Target.(*cred.Aws)
	if !ok {
		return nil, fmt.Errorf("expected :%T, but had %T", ret, secret.Target)
	}
	return ret, nil
}

func putSqsMessageAttributes(attributes map[string]interface{}, target map[string]types.MessageAttributeValue) {
	for k, v := range attributes {
		if v == nil {
			continue
		}
		dataType := getAttributeDataType(v)
		target[k] = types.MessageAttributeValue{
			DataType:    &dataType,
			StringValue: aws.String(toolbox.AsString(v)),
		}
	}
}

func getAttributeDataType(value interface{}) string {
	dataType := "String"
	if toolbox.IsInt(value) || toolbox.IsFloat(value) {
		dataType = "Number"
	}
	return dataType
}

func New() *Service {
	return &Service{
		resources: make(map[string]*mbus.Resource),
	}
}
