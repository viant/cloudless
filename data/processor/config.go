package processor

import (
	"bufio"
	"context"
	"errors"
	"github.com/viant/afs"
	"github.com/viant/tapper/config"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	//OnDoneDelete delete action
	OnDoneDelete = "delete"
	//OnDoneMove move action
	OnDoneMove = "move"
)

// Config represents processor configuration
type (
	Config struct {
		DeadlineReductionMs int // Deadline typically comes from Lambda ctx. Max exec time == Deadline - DeadlineReductionMs
		LoaderDeadlineLagMs int // Loader will finish earlier than workers to let the latter complete
		MaxRetries          int
		Concurrency         int
		DestinationURL      string // Service processing data destination URL. This is a template, e.g. $gs://$mybucket/$prefix/$a.dat
		DestinationCodec    string
		Destination         *config.Stream
		RetryURL            string // destination for the data to be retried
		FailedURL           string // destination for the data that has failed max retires
		CorruptionURL       string /// destination for the corrupted data
		MaxExecTimeMs       int    // default execution timeMs used when context does not come with deadline
		OnDone              string //move or delete, (move moves data to process URL,or delete for delete)
		OnDoneURL           string
		ReaderBufferSize    int    //if set above zero uses afs Steam option
		BatchSize           int    //number of data lines passed to processor (1 by default)
		Sort                Sort   //optional sorting config
		ScannerBufferMB     int    //use in case you see bufio.Scanner: token too long
		MetricPort          int    //if specified HTTP endpoint port to expose metrics
		RowTypeName         string // parquet/json row type
	}
)

func (c Config) ExpandDestinationURL(startTime time.Time) string {
	if c.Destination != nil && c.Destination.URL != "" {
		return expandURL(c.Destination.URL, startTime)
	}
	return expandURL(c.DestinationURL, startTime)
}

func (c Config) ExpandDestinationRotationURL(startTime time.Time) string {
	if c.Destination != nil && c.Destination.Rotation != nil && c.Destination.Rotation.URL != "" {
		return expandURL(c.Destination.Rotation.URL, startTime)
	}
	return ""
}

func (c *Config) ExpandDestination(startTime time.Time) *config.Stream {
	if c.Destination == nil && c.DestinationURL == "" {
		return nil
	}
	destination := &config.Stream{}
	destination.URL = c.ExpandDestinationURL(startTime) //
	if c.DestinationCodec != "" {
		destination.Codec = c.DestinationCodec
	}
	if c.Destination != nil && c.Destination.Rotation != nil {
		rotation := &config.Rotation{}
		rotation.URL = c.ExpandDestinationRotationURL(startTime)
		rotation.Codec = c.Destination.Rotation.Codec
		rotation.EveryMs = c.Destination.Rotation.EveryMs
		rotation.MaxEntries = c.Destination.Rotation.MaxEntries
		rotation.Emit = c.Destination.Rotation.Emit
		destination.Rotation = rotation
		if c.Destination.URL == "" && c.Destination.Rotation.URL != "" {
			destination.URL = rotation.URL
		}
	}
	return destination
}

// Deadline returns max execution time for a Processor
func (c Config) Deadline(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(time.Duration(c.MaxExecTimeMs) * time.Millisecond)
		if timeoutSec := os.Getenv("FUNCTION_TIMEOUT_SEC"); timeoutSec != "" {
			if timeSec, err := strconv.Atoi(timeoutSec); err == nil && timeSec > 1 {
				deadline = time.Now().Add(time.Duration(timeSec-1) * time.Second)
			}
		}
	}
	return deadline.Add(-time.Millisecond * time.Duration(c.DeadlineReductionMs))
}

// Deadline returns max execution time for a Processor
func (c Config) LoaderDeadline(ctx context.Context) time.Time {
	return c.Deadline(ctx).Add(-time.Millisecond * time.Duration(c.LoaderDeadlineLagMs))
}

//Validate checks if Config is valid
func (c *Config) Validate() error {
	if c.RetryURL == "" {
		return errors.New("retryURL was empty")
	}
	if c.FailedURL == "" {
		return errors.New("failedURL was empty")
	}
	if c.CorruptionURL == "" {
		return errors.New("corruptionURL was empty")
	}
	if c.MaxExecTimeMs > math.MaxInt32 {
		return errors.New("maxExecTimeMs too large")
	}
	return nil
}

//InitWithNoLimit intialise config with no execution limit
func (c *Config) InitWithNoLimit() {
	c.RetryURL = "mem://localhost/retry"
	c.FailedURL = "mem://localhost/failed"
	c.CorruptionURL = "mem://localhost/corruption"
	c.MaxExecTimeMs = math.MaxInt32
}

//Init sets default Config values
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	if c.MaxExecTimeMs == 0 {
		c.MaxExecTimeMs = 9 * 60000 //9 min
	}
	if c.DeadlineReductionMs == 0 { //by default 1% of execution
		c.DeadlineReductionMs = int(float64(c.MaxExecTimeMs) * 0.01)
	}
	if c.LoaderDeadlineLagMs == 0 { //by default 1% of execution
		c.LoaderDeadlineLagMs = int(float64(c.MaxExecTimeMs) * 0.01)
	}
	if c.DestinationCodec == "gzip" && !strings.HasSuffix(c.DestinationURL, ".gz") {
		c.DestinationURL += ".gz"
	}
	if c.DestinationCodec == "" && strings.HasSuffix(c.DestinationURL, ".gz") {
		c.DestinationCodec = "gzip"
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 10
	}
	if c.Concurrency == 0 {
		c.Concurrency = 20
	}
	return nil
}

func (c Config) AdjustScannerBuffer(scanner *bufio.Scanner) {
	if c.ScannerBufferMB > 0 {
		scanner.Buffer(make([]byte, 0, 64*1024), c.ScannerBufferMB*1024*1024)
	}
}
