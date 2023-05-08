package mbus

import (
	"context"
)

type Service interface {
	Push(ctx context.Context, dest *Resource, message *Message) (*Confirmation, error)
}
