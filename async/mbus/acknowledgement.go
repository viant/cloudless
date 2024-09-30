package mbus

import "fmt"

var isAck = true
var nack = false

// Acknowledgement represents message acknowledgement
type Acknowledgement struct {
	isAck *bool
	Error error
}

func (c *Acknowledgement) Ack() error {
	if c.isAck != nil {
		return fmt.Errorf("already acknowledged")
	}
	c.isAck = &isAck
	return nil
}

func (c *Acknowledgement) Nack() error {
	if c.isAck != nil {
		return fmt.Errorf("already acknowledged")
	}
	c.isAck = &nack
	return nil
}

func (c *Acknowledgement) IsAck() bool {
	return c.isAck != nil && *c.isAck
}

func (c *Acknowledgement) IsNack() bool {
	return c.isAck != nil && !*c.isAck
}

func NewAcknowledgement() *Acknowledgement {
	return &Acknowledgement{}
}
