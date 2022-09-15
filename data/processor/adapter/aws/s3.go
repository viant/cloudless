package aws

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	_ "github.com/viant/afsc/s3"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
	"github.com/viant/cloudless/row_type"
	"io"
	"strings"
	"time"
)

//S3Event represents S3 Events
type S3Event struct {
	events.S3Event
}

//NewRequest creates processing request
func (e S3Event) NewRequest(ctx context.Context, fs afs.Service, cfg *processor.Config) (*processor.Request, error) {
	URL := fmt.Sprintf("s3://%s/%s", e.Records[0].S3.Bucket.Name, e.Records[0].S3.Object.Key)

	request := &processor.Request{}
	if strings.HasSuffix(URL, ".parquet") {
		request.SourceType = processor.Parquet
	} else if strings.HasSuffix(URL, ".json") || strings.HasSuffix(URL, ".json.gz") {
		request.SourceType = processor.JSON
	} else {
		request.SourceType = processor.CSV
	}

	if request.SourceType == processor.CSV || request.SourceType == processor.JSON {
		var options = make([]storage.Option, 0)
		if cfg.ReaderBufferSize > 0 {
			object, err := fs.Object(ctx, URL)
			if err != nil {
				return nil, err
			}
			options = append(options, option.NewStream(cfg.ReaderBufferSize, int(object.Size())))
		}
		options = append(options, option.NewRegion(e.Records[0].AWSRegion))
		reader, err := ioutil.OpenURL(ctx, fs, URL, options...)
		if err != nil {
			return nil, err
		}
		request.ReadCloser = reader
		if request.SourceType == processor.JSON {
			request.RowType = row_type.RowType(cfg.RowTypeName)
		}
		if cfg.ReaderBufferSize == 0 {
			buf := new(bytes.Buffer)
			if _, err := io.Copy(buf, reader); err != nil {
				return nil, err
			}
			reader.Close()
			request.ReadCloser = io.NopCloser(bytes.NewReader(buf.Bytes()))
		}
	} else { // Parquet
		if request.RowType = row_type.RowType(cfg.RowTypeName); request.RowType == nil {
			return nil, fmt.Errorf(" parquet type name '%s' not registered", cfg.RowTypeName)
		}
		buffer, err := fs.DownloadWithURL(ctx, URL)
		if err != nil {
			return nil, err
		}
		request.ReaderAt = bytes.NewReader(buffer)
	}
	request.SourceURL = URL
	request.StartTime = time.Now()

	return request, nil
}
