package destination

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
)

//DataLoggerKey data logger key
type dataLoggerKey string

//DataLoggerKey data logger context key
const DataLoggerKey = dataLoggerKey("dataLogger")

//NewDataLogger creates a data logger
func NewDataLogger(ctx context.Context, reporter processor.Reporter) (context.Context, error) {
	baseResponse := reporter.BaseResponse()
	cfg := &config.Stream{
		URL:          baseResponse.Destination.URL,
		Codec:        baseResponse.Destination.Codec,
		Rotation:     baseResponse.Destination.Rotation,
		StreamUpload: true,
	}
	logger, err := log.New(cfg, "", afs.New())
	if err != nil {
		return nil, fmt.Errorf("failed to create logger with: %+v, due to %w", cfg, err)
	}
	return context.WithValue(ctx, DataLoggerKey, logger), nil
}
