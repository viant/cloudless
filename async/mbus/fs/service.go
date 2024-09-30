package mem

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/cloudless/async/mbus"
)

// Service in memory service
type Service struct {
	fs *fileSystem
}

func (s *Service) Push(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	switch dest.Type {
	case mbus.ResourceTypeQueue:
		return s.sendMessage(ctx, dest, message)
	}
	return nil, fmt.Errorf("unsupported resource type: %v", dest.Type)
}

func (s *Service) sendMessage(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	err := s.fs.write(ctx, dest, message)
	if err != nil {
		return nil, err
	}
	return &mbus.Confirmation{MessageID: message.ID}, nil
}

// New creates a new in memory service
func New() *Service {
	return &Service{
		fs: &fileSystem{fs: afs.New()},
	}
}
