package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/data/processor/adapter/gcp"
	"github.com/viant/cloudless/data/processor/stat"
	"github.com/viant/gmetric"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

//Service represents subscriber service
type Service struct {
	config    *Config
	client    *pubsub.Client
	processor *processor.Service
	fs        afs.Service
	stats     *gmetric.Operation
	messages  chan *pubsub.Message
	pending   int32
}

//Consume starts consumer
func (s *Service) Consume(ctx context.Context) error {
	for {
		err := s.consume(ctx)
		if err != nil {
			log.Printf("failed to consume: %v\n", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Service) consume(ctx context.Context) error {
	var subscription *pubsub.Subscription
	if s.config.ProjectID == "" {
		subscription = s.client.Subscription(s.config.Subscription)
	} else {
		subscription = s.client.SubscriptionInProject(s.config.Subscription, s.config.ProjectID)
	}
	batchSize := s.config.BatchSize - int(atomic.LoadInt32(&s.pending))
	if batchSize <= 0 {
		return nil
	}
	subscription.ReceiveSettings.MaxOutstandingMessages = batchSize
	subscription.ReceiveSettings.NumGoroutines = s.config.Concurrency
	subscription.ReceiveSettings.MaxExtension = time.Duration(s.config.VisibilityTimeout) * time.Second
	return subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		if msg == nil {
			return
		}
		if os.Getenv("DEBUG_MSG") == "1" {
			fmt.Printf("added message %v\n", string(msg.Data))
		}
		atomic.AddInt32(&s.pending, 1)
		s.handleMessage(ctx, msg, s.fs)
	})
}

func (s *Service) handleMessage(ctx context.Context, msg *pubsub.Message, fs afs.Service) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Printf("recover from panic: %v, %v", r, string(debug.Stack()))
		}
		atomic.AddInt32(&s.pending, -1)
	}()
	recentCounter, onDone, stats := stat.SubscriberBegin(s.stats)
	defer stat.SubscriberEnd(s.stats, recentCounter, onDone, stats)

	gsEvent := &gcp.GSEvent{}
	if err := json.Unmarshal(msg.Data, gsEvent); err != nil {
		log.Printf("failed to unmarshal GSEvent: %s, due to %v\n", msg.Data, err)
		msg.Nack()
		stats.Append(stat.NegativeAcknowledged)
		stats.Append(err)
		return
	}
	URL := gsEvent.URL()
	reqContext := context.Background()
	request, err := gsEvent.NewRequest(reqContext, s.fs, &s.config.Config)
	stats.Append(err)
	if err != nil {
		//source file has been removed
		if exists, _ := fs.Exists(ctx, URL); !exists {
			msg.Ack()
			stats.Append(stat.Acknowledged)
			return
		}
		log.Printf("failed to create process request from GSEvent: %s, due to %v\n", msg.Data, err)
		stats.Append(stat.NegativeAcknowledged)
		msg.Nack()
		return
	}
	reporter := s.processor.Do(reqContext, request)
	msg.Ack()
	stats.Append(stat.Acknowledged)
	output, err := json.Marshal(reporter)
	if err != nil {
		stats.Append(err)
		fmt.Printf("failed marshal reported %v\n", reporter)
	}
	if reporter != nil {
		if baseResponse := reporter.BaseResponse(); baseResponse != nil {
			if len(baseResponse.Errors) > 0 {
				stats.Append(fmt.Errorf(baseResponse.Errors[0]))
			}
			if baseResponse.CorruptionErrors > 0 {
				stats.Append(stat.DataCorruption)
			}
			if strings.Contains(baseResponse.SourceURL, processor.RetryFragment) {
				stats.Append(stat.Retry)
			}
		}
		if reqContext.Err() != nil {
			stats.Append(stat.Timeout)
		}

	}
	fmt.Printf("%s\n", output)
}

//New creates a new subscriber
func New(config *Config, client *pubsub.Client, processor *processor.Service, fs afs.Service) (*Service, error) {
	err := config.Init(context.Background(), fs)
	if err != nil {
		return nil, err
	}
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	srv := &Service{
		config:    config,
		client:    client,
		processor: processor,
		fs:        fs,
	}
	location := reflect.TypeOf(srv).PkgPath()
	srv.stats = srv.processor.Metrics.MultiOperationCounter(location, stat.SubscriberMetricName, "subscriber performance", time.Microsecond, time.Microsecond, 3, stat.NewSubscriber())
	if srv.config.MetricPort > 0 {
		srv.processor.StartMetricsEndpoint()
	}
	return srv, nil
}
