package mem

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/cloudless/async/mbus"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type (
	Observer struct {
		config *Config
		fs     afs.Service
		closed int32
	}
	Config struct {
		MaxJobs int
		URL     string
	}
)

func (s *Observer) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *Observer) Close() error {
	atomic.StoreInt32(&s.closed, 1)
	return nil
}

func (s *Observer) Observe(ctx context.Context, listener mbus.Listener) {
	if s.config.URL == "" {
		return
	}
	pending := make(map[string]bool)
	var mux sync.RWMutex
	var limiter chan bool
	if s.config.MaxJobs > 0 {
		limiter = make(chan bool, s.config.MaxJobs)
	}

	for {
		if s.IsClosed() {
			return
		}
		objects, _ := s.fs.List(ctx, s.config.URL, option.NewRecursive(true))
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
				defer func() {
					if limiter != nil {
						<-limiter
					}
					mux.Lock()
					delete(pending, object.URL())
					mux.Unlock()
				}()

				data, err := s.fs.DownloadWithURL(ctx, object.URL())
				if err != nil {
					log.Println(err)
					return
				}
				msg := &mbus.Message{}
				if err := json.Unmarshal(data, msg); err != nil {
					log.Println(err)
					return
				}

				if err = listener.OnMessage(context.Background(), msg); err != nil {
					log.Println(err)
				}

			}(objects[i])

		}
		time.Sleep(100 * time.Millisecond)
	}
}
