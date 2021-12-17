package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/data/processor/adapter/aws"
	"github.com/viant/cloudless/data/processor/stat"
	"github.com/viant/gmetric"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"
)

//Service represents sqs service
type Service struct {
	config    *Config
	sqsClient *sqs.SQS
	queueURL  *string
	processor *processor.Service
	fs        afs.Service
	stats     *gmetric.Operation
	messages  chan *sqs.Message
	pending   int32
}

//Consume starts consumer
func (s *Service) Consume(ctx context.Context) error {
	for {
		err := s.consume()
		if err != nil {
			log.Printf("failed to consume: %v\n", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Service) consume() error {
	waitTimeSeconds := int64(s.config.WaitTimeSeconds)
	visibilityTimeout := int64(s.config.VisibilityTimeout)
	batchSize := int64(s.config.BatchSize) - int64(atomic.LoadInt32(&s.pending))
	if batchSize <= 0 {
		return nil
	}
	if os.Getenv("DEBUG_MSG") == "1" {
		fmt.Printf("requesting batch size: %v\n", batchSize)
	}
	maxNumberOfMessages := batchSize
	if maxNumberOfMessages > 10 {
		maxNumberOfMessages = 10
	}
	msgs, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            s.queueURL,
		MaxNumberOfMessages: &maxNumberOfMessages,
		WaitTimeSeconds:     &waitTimeSeconds,
		VisibilityTimeout:   &visibilityTimeout,
	})

	if err != nil {
		return err
	}
	for i := range msgs.Messages {
		if os.Getenv("DEBUG_MSG") == "1" {
			fmt.Printf("added message %v\n", msgs.Messages[i])
		}
		atomic.AddInt32(&s.pending, 1)
		s.messages <- msgs.Messages[i]
	}
	return nil
}

func (s *Service) handleMessages() {
	for {
		msg := <-s.messages
		if os.Getenv("DEBUG_MSG") == "1" {
			fmt.Printf("consume message %+v\n", msg)
		}
		if msg == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go s.handleMessage(context.Background(), msg, s.fs)
	}
}

func (s *Service) deleteMessage(msg *sqs.Message) error {
	_, err := s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      s.queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

func (s *Service) handleMessage(ctx context.Context, msg *sqs.Message, fs afs.Service) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Printf("recover from panic: %v, %v", r, string(debug.Stack()))
		}
		atomic.AddInt32(&s.pending, -1)
	}()
	if msg.Body == nil {
		return
	}
	recentCounter, onDone, stats := stat.SubscriberBegin(s.stats)
	defer stat.SubscriberEnd(s.stats, recentCounter, onDone, stats)

	s3Event := &aws.S3Event{}
	if err := json.Unmarshal([]byte(*msg.Body), s3Event); err != nil {
		log.Printf("failed to unmarshal S3vent: %s, due to %v\n", *msg.Body, err)
		stats.Append(err)
		return
	}
	if len(s3Event.Records) == 0 {
		err := s.deleteMessage(msg)
		if err != nil {
			stats.Append(stat.NegativeAcknowledged)
		} else {
			stats.Append(stat.Acknowledged)
		}
		fmt.Printf("invalid event: %s\n", *msg.Body)
		return
	}
	if os.Getenv("DEBUG_MSG") != "" {
		fmt.Printf("%s\n", *msg.Body)
	}
	reqContext := context.Background()
	request, err := s3Event.NewRequest(reqContext, s.fs, &s.config.Config)
	if err != nil {
		//source file has been removed
		sourceURL := ""
		if len(s3Event.Records) > 0 {
			sourceURL = fmt.Sprintf("s3://%s/%s", s3Event.Records[0].S3.Bucket.Name, s3Event.Records[0].S3.Object.Key)
		}
		if sourceURL != "" {
			if exists, _ := fs.Exists(ctx, sourceURL); !exists {
				if err = s.deleteMessage(msg); err != nil {
					stats.Append(err)
					stats.Append(stat.NegativeAcknowledged)
				} else {
					stats.Append(stat.Acknowledged)
				}
				return
			}
		}
		stats.Append(err)
		stats.Append(stat.NegativeAcknowledged)
		log.Printf("failed to create process request from s3Event: %s, due to %v\n", *msg.Body, err)
		return
	}
	reporter := s.processor.Do(reqContext, request)
	err = s.deleteMessage(msg)
	if err != nil {
		stats.Append(err)
		stats.Append(stat.NegativeAcknowledged)
	} else {
		stats.Append(stat.Acknowledged)
	}
	output, err := json.Marshal(reporter)
	if err != nil {
		stats.Append(err)
		fmt.Printf("failed marshal reported %v\n", reporter)
	}
	fmt.Printf("%s\n", output)
}

//New creates a new sqsService
func New(config *Config, client *sqs.SQS, processor *processor.Service, fs afs.Service) (*Service, error) {
	err := config.Init(context.Background(), fs)
	if err != nil {
		return nil, err
	}
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	result, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &config.QueueName,
	})
	if err != nil {
		return nil, err
	}
	srv := &Service{
		config:    config,
		sqsClient: client,
		queueURL:  result.QueueUrl,
		processor: processor,
		fs:        fs,
		messages:  make(chan *sqs.Message, config.BatchSize),
	}
	if srv.config.MetricPort > 0 {
		srv.processor.StartMetricsEndpoint()
	}
	go srv.handleMessages()
	location := reflect.TypeOf(srv).PkgPath()
	srv.stats = srv.processor.Metrics.MultiOperationCounter(location, stat.SubscriberMetricName, "subscriber performance", time.Microsecond, time.Microsecond, 3, stat.NewSubscriber())
	return srv, nil
}
