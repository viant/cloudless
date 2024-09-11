package mem

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/async/mbus"
	"sync"
	"sync/atomic"
	"time"
)

type (
	Notifier struct {
		fs      afs.Service
		stopped int32
	}

	pendings struct {
		registry map[string]bool
		mux      sync.RWMutex
	}
)

func (p *pendings) add(URL string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.registry[URL] = true

}

func (p *pendings) remove(URL string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	delete(p.registry, URL)
}

func (p *pendings) has(URL string) bool {
	p.mux.RLock()
	defer p.mux.RUnlock()
	_, has := p.registry[URL]
	return has
}

func (n *Notifier) IsClosed() bool {
	return atomic.LoadInt32(&n.stopped) == 1
}

func (n *Notifier) Stop() error {
	atomic.StoreInt32(&n.stopped, 1)
	return nil
}

func (n *Notifier) Observe(ctx context.Context, messanger mbus.Messenger, opts ...mbus.NotifierOption) error {
	options := mbus.NewNotifierOptions(opts...)

	URL := options.Resource.URL
	if URL == "" {
		return fmt.Errorf("URL was empty")
	}
	var nacks = make(map[string]int)
	pending := &pendings{registry: map[string]bool{}}
	var limiter chan bool
	if maxPending := options.MaxPending; maxPending > 0 {
		limiter = make(chan bool, maxPending)
	}
	if n.IsClosed() {
		return fmt.Errorf("notifier is closed")
	}

	for {
		if n.IsClosed() {
			return nil
		}
		objects, _ := n.fs.List(ctx, URL)
		objectCount := 0
		if len(objects) == 0 {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		for _, object := range objects {
			if object.IsDir() {
				continue
			}
			objectCount++
		}
		if objectCount == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for i, object := range objects {
			if object.IsDir() {
				continue
			}
			if pending.has(object.URL()) {
				continue
			}

			pending.add(object.URL())

			if limiter != nil {
				limiter <- true
			}

			anObject := objects[i]
			go func(object storage.Object) {
				ack := &mbus.Acknowledgement{}
				defer func() {
					if limiter != nil {
						<-limiter
					}
					if ack.IsNack() {
						n.handleNack(object, nacks, pending)
					} else if ack.IsAck() {
						n.handleAck(ctx, object, pending)
					}
				}()

				data, err := n.fs.DownloadWithURL(ctx, object.URL())
				if err != nil {
					ack.Error = err
					_ = ack.Nack()
					return
				}
				msg := &mbus.Message{Data: make(map[string]interface{})}
				if err := json.Unmarshal(data, msg); err != nil {
					ack.Error = err
					_ = ack.Nack()
					return
				}

				if err = messanger.OnMessage(context.Background(), msg, ack); err == nil {
					if !(ack.IsAck() || ack.IsNack()) { //if nothing is flagged, we ack
						err = ack.Ack()
					}
				}
				if err != nil {
					_ = ack.Nack()
				}

			}(anObject)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Notifier) handleAck(ctx context.Context, object storage.Object, pending *pendings) {
	n.fs.Delete(ctx, object.URL())
	pending.remove(object.URL())
}

func (n *Notifier) handleNack(object storage.Object, nacks map[string]int, pending *pendings) {
	nacks[object.URL()]++
	if nacks[object.URL()] > 3 {
		parentPath, _ := url.Split(object.URL(), file.Scheme)
		_ = n.fs.Move(context.Background(), object.URL(), url.Join(parentPath, "nack", object.Name()))
		pending.remove(object.URL())
	}
}

func (n *Notifier) Notify(ctx context.Context, messenger mbus.Messenger, options ...mbus.NotifierOption) error {
	return n.Observe(ctx, messenger, options...)
}

func NewNotifier() mbus.Notifier {
	return &Notifier{
		fs: afs.New(),
	}
}
