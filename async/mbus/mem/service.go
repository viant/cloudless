package mem

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/cloudless/async/mbus"
)

// Service in memory service
type Service struct{}

func (s *Service) Push(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	switch dest.Type {
	case mbus.ResourceTypeQueue:
		return s.sendMessage(ctx, dest, message)
	}
	return nil, fmt.Errorf("unsupported resource type: %v", dest.Type)
}

func (s *Service) sendMessage(ctx context.Context, dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	select {
	case Singleton().Queue(dest) <- message:
		message.ID = uuid.New().String()
		return &mbus.Confirmation{MessageID: message.ID}, nil
	default:
		return nil, fmt.Errorf("failed to send message: %v", message)
	}
}

// New creates a new in memory service
func New() *Service {
	return &Service{}
}
