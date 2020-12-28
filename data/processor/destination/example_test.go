package destination_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/data/processor/destination"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	"strings"
)

//Transformer transform csv into JSON
type Transformer struct {
	msgProvider *msg.Provider
}

//Pre initialize data logger for config.DestinationURL
func (p Transformer) Pre(ctx context.Context, reporter processor.Reporter) (context.Context, error) {
	return destination.NewDataLogger(ctx, reporter)
}

//Process transform CSV into JSON
func (p Transformer) Process(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	csvReader := csv.NewReader(bytes.NewReader(data))
	record, err := csvReader.Read()
	if err == nil && len(record) < 3 {
		err = fmt.Errorf("invalid record size, expected 3 but had: %v", len(record))
	}
	if err != nil {
		return processor.NewDataCorruption(fmt.Sprintf("failed to read record: %s, %v", data, err))
	}
	message := p.msgProvider.NewMessage()
	defer message.Free()
	message.PutString("ID", record[0])
	message.PutString("Region", record[1])
	message.PutStrings("SegmentIDS", strings.Split(record[2], ","))
	logger := ctx.Value(destination.DataLoggerKey).(*log.Logger)
	return logger.Log(message)
}

//Post closes logger and finalize data upload to the data destination
func (p Transformer) Post(ctx context.Context, reporter processor.Reporter) error {
	logger := ctx.Value(destination.DataLoggerKey).(*log.Logger)
	return logger.Close()
}

//NewTransformer creates a new transformer
func NewTransformer() *Transformer {
	return &Transformer{
		msgProvider: msg.NewProvider(16*1024, 20),
	}
}
