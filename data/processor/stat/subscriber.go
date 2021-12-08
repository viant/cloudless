package stat

import (
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/counter"
	"time"
)

const (
	ErrorKey             = "error"
	DataCorruption       = "data_corruption"
	Pending              = "pending"
	Retry                = "retry"
	Timeout              = "timeout"
	Acknowledged         = "ack"
	NegativeAcknowledged = "nack"
	SubscriberMetricName = "subscriber"
)

func SubscriberBegin(operation *gmetric.Operation) (*counter.Operation, counter.OnDone, *Values) {
	startTime := time.Now()
	onDone := operation.Begin(startTime)
	stats := NewValues()
	operation.IncrementValue(Pending)
	recentCounter := operation.Recent[operation.Index(startTime)]
	recentCounter.IncrementValue(Pending)
	return recentCounter, onDone, stats
}

func SubscriberEnd(operation *gmetric.Operation, recentCounter *counter.Operation, onDone counter.OnDone, stats *Values) {
	operation.DecrementValue(Pending)
	recentCounter.DecrementValue(Pending)
	onDone(time.Now(), stats.Values()...)
}

type subscriber struct {
}

func (p subscriber) Keys() []string {
	return []string{
		ErrorKey,
		DataCorruption,
		Pending,
		Timeout,
		Retry,
		Acknowledged,
		NegativeAcknowledged,
	}
}

func (p subscriber) Map(value interface{}) int {
	if value == nil {
		return -1
	}
	if _, ok := value.(error); ok {
		return 0
	}
	switch value {
	case ErrorKey:
		return 0
	case DataCorruption:
		return 1
	case Pending:
		return 2
	case Timeout:
		return 3
	case Retry:
		return 4
	case Acknowledged:
		return 5
	case NegativeAcknowledged:
		return 6

	}
	return -1
}

func NewSubscriber() counter.Provider {
	return &subscriber{}
}
