package gcp

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	_ "github.com/viant/afsc/gs"
	"github.com/viant/cloudless/data/processor"
	"github.com/viant/cloudless/ioutil"
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
			"GSEvent": e,
		},
		SourceURL: URL,
		StartTime: time.Now(),
	}
	return request, nil
}
