package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/data/processor/adapter/gcp"
	"log"
	"os"
)

//Service represents subscriber service
type Service struct {
	config    *Config
	client    *pubsub.Client
	processor *processor.Service
	fs        afs.Service
}

//Consume starts consumer
func (s *Service) Consume(ctx context.Context) error {
	for {
		err := s.consume(ctx)
		if err != nil {
			log.Printf("failed to consume: %v\n", err)
		}
	}
}

func (s *Service) consume(ctx context.Context) error {
	var URL string
	defer func() {
		r := recover()
		if r != nil {
			fmt.Printf("recover from panic: URL:%v, error: %v", URL, r)
		}
	}()
	var subscription *pubsub.Subscription
	if s.config.ProjectID == "" {
		subscription = s.client.Subscription(s.config.Subscription)
	} else {
		subscription = s.client.SubscriptionInProject(s.config.Subscription, s.config.ProjectID)
	}
	fs := afs.New()
	subscription.ReceiveSettings.MaxOutstandingMessages = s.config.BatchSize
	subscription.ReceiveSettings.NumGoroutines = s.config.BatchSize
	return subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		s.handleMessage(ctx, msg, URL, fs)
	})
}

func (s *Service) handleMessage(ctx context.Context, msg *pubsub.Message, URL string, fs afs.Service) {
	gsEvent := &gcp.GSEvent{}
	if err := json.Unmarshal(msg.Data, gsEvent); err != nil {
		log.Printf("failed to unmarshal GSEvent: %s, due to %v\n", msg.Data, err)
		msg.Nack()
		return
	}
	URL = gsEvent.URL()
	if os.Getenv("DEBUG_MSG") != "" {
		fmt.Printf("%s\n", msg.Data)
	}
	reqContext := context.Background()
	request, err := gsEvent.NewRequest(reqContext, s.fs, &s.config.Config)
	if err != nil {
		//source file has been removed
		if exists, _ := fs.Exists(ctx, URL); !exists {
			msg.Ack()
			return
		}
		log.Printf("failed to create process request from GSEvent: %s, due to %v\n", msg.Data, err)
		msg.Nack()
		return
	}
	reporter := s.processor.Do(reqContext, request)
	msg.Ack()
	output, err := json.Marshal(reporter)
	if err != nil {
		fmt.Printf("failed marshal reported %v\n", reporter)
	}
	fmt.Printf("%s\n", output)
}

//New creates a new subscriber
func New(config *Config, client *pubsub.Client, processor *processor.Service, fs afs.Service) *Service {
	return &Service{
		config:    config,
		client:    client,
		processor: processor,
		fs:        fs,
	}
}
