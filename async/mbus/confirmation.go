package mbus

type Confirmation struct {
	MessageID string
}

func (c Confirmation) String() string {
	return c.MessageID
}
