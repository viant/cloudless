package mem

import (
	"github.com/viant/cloudless/async/mbus"
	"sync"
)

type Queues struct {
	queue map[string]chan *mbus.Message
	sync.Mutex
}

func (m *Queues) Queue(resource *mbus.Resource) chan *mbus.Message {
	m.Lock()
	defer m.Unlock()
	if m.queue == nil {
		m.queue = make(map[string]chan *mbus.Message)
	}
	if _, ok := m.queue[resource.Name]; !ok {
		m.queue[resource.Name] = make(chan *mbus.Message, 10000)
	}
	return m.queue[resource.Name]
}

var queues = &Queues{queue: map[string]chan *mbus.Message{}}

// Singleton returns message queue
func Singleton() *Queues {
	return queues
}
