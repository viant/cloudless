package mbus

import "encoding/json"

type Message struct {
	ID         string
	Resource   *Resource
	TraceID    string
	Attributes map[string]interface{}
	Data       interface{}
}

func (m *Message) Payload() ([]byte, error) {
	if m.Data == nil {
		return nil, nil
	}
	switch actual := m.Data.(type) {
	case string:
		return []byte(actual), nil
	case []byte:
		return actual, nil
	default:
		return json.Marshal(m.Data)
	}
}

func (m *Message) AddAttribute(name string, value interface{}) {
	if len(m.Attributes) == 0 {
		m.Attributes = make(map[string]interface{})
	}
	m.Attributes[name] = value
}
