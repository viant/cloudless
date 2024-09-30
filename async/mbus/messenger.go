package mbus

import "context"

// Messenger represents message listener
type (
	Messenger interface {
		//OnMessage handles message
		OnMessage(ctx context.Context, message *Message, ack *Acknowledgement) error
	}
)
