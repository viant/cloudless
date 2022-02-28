package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"log"
	"os"
	"testing"
	"time"
)

func TestService_Consume(t *testing.T) {
	config := &Config{
		Config:       processor.Config{
			OnDone: "delete",
		},
		ProjectID:    "viant-e2e",
		Subscription: "cloudless-sub",
		BatchSize: 5,
		Concurrency: 2,
		MaxExtension: 2 * time.Minute,
	}
	os.Setenv("DEBUG_MSG","1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS","secret.json")
	fs := afs.New()
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx,config.ProjectID)
	if !assert.Nil(t, err) {
		log.Println(err)
		return
	}
	procService := processor.New(&config.Config,fs,&testProcessor{},processor.NewReporter)
	pubsubService,err := New(config,client,procService,fs)
	if !assert.Nil(t, err) {
		log.Println(err)
		return
	}
	err = pubsubService.Consume(ctx)
	if !assert.Nil(t, err) {
		log.Println(err)
		return
	}	
}


type testProcessor struct {
}

func (p *testProcessor) Pre(ctx context.Context, reporter processor.Reporter) (context.Context, error) {
	return ctx, nil
}
func (p *testProcessor) Process(ctx context.Context, data []byte, reporter processor.Reporter) error {
	return nil
}
func (p *testProcessor) Post(ctx context.Context, reporter processor.Reporter) error {
	return nil
}

