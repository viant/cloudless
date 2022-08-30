package gcp

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	_ "github.com/viant/afsc/gs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
	"github.com/viant/cloudless/row_type"
	"strings"
	"time"
)

//GSEvent represents GS event
type GSEvent struct {
	Bucket                  string `json:"bucket"`
	Name                    string `json:"name"`
	ContentType             string `json:"contentType"`
	CRC32C                  string `json:"crc32c"`
	Etag                    string `json:"etag"`
	Generation              string `json:"generation"`
	ID                      string `json:"id"`
	Kind                    string `json:"kind"`
	Md5Hash                 string `json:"md5Hash"`
	MediaLink               string `json:"mediaLink"`
	Metageneration          string `json:"metageneration"`
	SelfLink                string `json:"selfLink"`
	Size                    string `json:"size"`
	StorageClass            string `json:"storageClass"`
	TimeCreated             string `json:"timeCreated"`
	TimeStorageClassUpdated string `json:"timeStorageClassUpdated"`
	Updated                 string `json:"updated"`
}

//URL returns sourceURL
func (e GSEvent) URL() string {
	return fmt.Sprintf("gs://%s/%s", e.Bucket, e.Name)
}

//NewRequest creates processing request
func (e GSEvent) NewRequest(ctx context.Context, fs afs.Service, cfg *processor.Config) (*processor.Request, error) {
	URL := fmt.Sprintf("gs://%s/%s", e.Bucket, e.Name)

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
		reader, err := ioutil.OpenURL(ctx, fs, URL, options...)
		if err != nil {
			return nil, err
		}
		request.ReadCloser = reader
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
