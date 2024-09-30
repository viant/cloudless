package destination

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"strings"
	"sync"
)

// MultiLogger represents a multi logger
type MultiLogger struct {
	mux      sync.RWMutex
	loggers  map[string]*log.Logger
	reporter processor.Reporter
	keyName  string
}

// Get gets or creates a new logger
func (m *MultiLogger) Get(key string) (*log.Logger, error) {
	m.mux.RLock()
	logger, ok := m.loggers[key]
	m.mux.RUnlock()
	if ok {
		return logger, nil
	}

	m.mux.Lock()
	defer m.mux.Unlock()
	if logger, ok = m.loggers[key]; ok {
		return logger, nil
	}

	baseResponse := m.reporter.BaseResponse()
	URL := baseResponse.Destination.URL
	URL = m.ReplaceKeyName(URL, key)

	rotation := baseResponse.Destination.Rotation
	var aRotation *config.Rotation
	if rotation != nil {
		aRotation = &config.Rotation{
			EveryMs:    rotation.EveryMs,
			MaxEntries: rotation.MaxEntries,
			URL:        m.ReplaceKeyName(rotation.URL, key),
			Codec:      rotation.Codec,
			Emit:       rotation.Emit,
		}
	}
	cfg := &config.Stream{
		URL:          URL,
		Codec:        baseResponse.Destination.Codec,
		Rotation:     aRotation,
		StreamUpload: true,
	}
	var err error
	logger, err = log.New(cfg, "", afs.New())
	if err != nil {
		return nil, err
	}
	m.loggers[key] = logger
	return logger, nil
}

func (m *MultiLogger) ReplaceKeyName(URL string, key string) string {
	if count := strings.Count(URL, m.keyName); count > 0 {
		URL = strings.Replace(URL, m.keyName, key, count)
	}
	return URL
}

// Stop closes all loggers
func (m *MultiLogger) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	for _, logger := range m.loggers {
		if e := logger.Close(); e != nil {
			err = e
		}
	}
	return err
}

// DataLoggerKey data logger key
type dataMultiLoggerKey string

// DataLoggerKey data logger context key
const DataMultiLoggerKey = dataMultiLoggerKey("dataMultiLogger")

// NewDataMultiLogger creates a data multi logger
func NewDataMultiLogger(ctx context.Context, keyName string, reporter processor.Reporter) (context.Context, error) {
	result := &MultiLogger{
		keyName:  keyName,
		reporter: reporter,
		loggers:  map[string]*log.Logger{},
	}
	return context.WithValue(ctx, DataMultiLoggerKey, result), nil
}
