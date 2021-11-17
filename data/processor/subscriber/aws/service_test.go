package aws

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"testing"
)

func TestService(t *testing.T) {
	cfg := Config{
		Config:    processor.Config{},
		QueueName: "s3_queue",
	}

	fs := afs.New()
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sqsClient := sqs.New(sess)
	_, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &cfg.QueueName,
	})
	if !assert.Nil(t, err, "queue create failed") {
		return
	}

	procService := processor.New(&cfg.Config, fs, &emptyProcessor{}, processor.NewReporter)
	sqsService, err := New(&cfg, sqsClient, procService, fs)
	if !assert.Nil(t, err, "sqsService create failed") {
		return
	}
	sqsService.Consume(context.Background())
}

type emptyProcessor struct {
}

func (p *emptyProcessor) Pre(ctx context.Context, reporter processor.Reporter) (context.Context, error) {
	return ctx, nil
}
func (p *emptyProcessor) Process(ctx context.Context, data []byte, reporter processor.Reporter) error {
	return nil
}
func (p *emptyProcessor) Post(ctx context.Context, reporter processor.Reporter) error {
	return nil
}
