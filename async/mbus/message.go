package mbus

import (
	"encoding/json"
	"strconv"
)

type Message struct {
	ID         string
	Resource   *Resource
	TraceID    string
	Attributes map[string]interface{}
	Subject    string
	Data       interface{}
}

func (m *Message) Payload() ([]byte, error) {
	if m.Data == nil {
		return nil, nil
	}
	switch actual := m.Data.(type) {
	case int:
		return []byte(strconv.Itoa(actual)), nil
	case float64:
		return []byte(strconv.FormatFloat(actual, 'f', 32, 5)), nil
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
