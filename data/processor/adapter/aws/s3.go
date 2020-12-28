package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	_ "github.com/viant/afsc/s3"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
	"time"
)

//S3Event represents S3 Events
type S3Event struct {
	events.S3Event
}

//NewRequest creates processing request
func (e S3Event) NewRequest(ctx context.Context, fs afs.Service, cfg *processor.Config) (*processor.Request, error) {
	URL := fmt.Sprintf("s3://%s/%s", e.Records[0].S3.Bucket.Name, e.Records[0].S3.Object.Key)
	var options = make([]storage.Option, 0)
	if cfg.ReaderBufferSize > 0 {
		object, err := fs.Object(ctx, URL)
		if err != nil {
			return nil, err
		}
		options = append(options, option.NewStream(cfg.ReaderBufferSize, int(object.Size())))
	}
	reader, err := ioutil.OpenURL(ctx, fs, URL, options...)
	if err != nil {
		return nil, err
	}
	request := &processor.Request{
		ReadCloser: reader,
		Attrs: map[string]interface{}{
			"S3EventRecord": e.Records[0],
		},
		SourceURL: URL,
		StartTime: time.Now(),
	}
	return request, nil
}
