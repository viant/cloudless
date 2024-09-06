package mbus

import "context"

// Listener represents message listener
type Listener interface {
	//OnMessage handles message
	OnMessage(ctx context.Context, message *Message) error
}
