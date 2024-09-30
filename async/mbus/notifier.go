package mbus

import "context"

type (
	NotifierOptions struct {
		//MaxPending max messages to process
		MaxPending int
		Resource   *Resource
	}
	NotifierOption func(Notifier *NotifierOptions)
)

func NewNotifierOptions(options ...NotifierOption) *NotifierOptions {
	var result = &NotifierOptions{}
	for _, option := range options {
		option(result)
	}
	return result
}

// WithMaxMessages sets max messages to process
func WithMaxMessages(value int) NotifierOption {
	return func(options *NotifierOptions) {
		options.MaxPending = value
	}
}

// WithResource sets resource
func WithResource(value *Resource) NotifierOption {
	return func(options *NotifierOptions) {
		options.Resource = value
	}
}

// Notifier represents message Notifier
type Notifier interface {
	Notify(ctx context.Context, messenger Messenger, options ...NotifierOption) error
}
