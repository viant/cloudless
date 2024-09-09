package mem

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
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
)

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
	pending := make(map[string]bool)
	var mux sync.RWMutex
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
		objects, _ := n.fs.List(ctx, URL, option.NewRecursive(true))
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
			mux.RLock()
			isPending := pending[object.URL()]
			mux.RUnlock()
			if isPending {
				continue
			}
			mux.Lock()
			pending[object.URL()] = true
			mux.Unlock()

			if limiter != nil {
				limiter <- true
			}

			go func(object storage.Object) {
				ack := &mbus.Acknowledgement{}
				defer func() {
					if limiter != nil {
						<-limiter
					}
					mux.Lock()
					if ack.IsAck() {
						delete(pending, object.URL())
					}
					mux.Unlock()
				}()

				data, err := n.fs.DownloadWithURL(ctx, object.URL())
				if err != nil {
					ack.Error = err
					return
				}
				msg := &mbus.Message{}
				if err := json.Unmarshal(data, msg); err != nil {
					ack.Error = err
					return
				}

				if err = messanger.OnMessage(context.Background(), msg, ack); err == nil {
					if !(ack.IsAck() || ack.IsNack()) { //if nothing is flagged, we ack
						err = ack.Ack()
					}
				}
			}(objects[i])
		}
		time.Sleep(100 * time.Millisecond)
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
