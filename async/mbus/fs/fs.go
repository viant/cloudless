package mem

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/async/mbus"
)

type fileSystem struct {
	fs afs.Service
}

func (s *fileSystem) write(ctx context.Context, resource *mbus.Resource, message *mbus.Message) error {
	if message.ID == "" {
		message.ID = uuid.New().String()
	}
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	URL := url.Join(resource.URL, message.ID+".msg")
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}
