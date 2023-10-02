package resource

import (
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type Tracker struct {
	watchURL       string
	assets         Assets
	mutex          sync.Mutex
	checkFrequency time.Duration
	nextCheck      time.Time
}

func (m *Tracker) isCheckDue(now time.Time) bool {
	if m.nextCheck.IsZero() || now.After(m.nextCheck) {
		m.nextCheck = now.Add(m.checkFrequency)
		return true
	}
	return false
}

func (m *Tracker) hasChanges(assets []storage.Object) bool {
	if len(assets) != len(m.assets) {
		return true
	}
	for _, asset := range assets {
		if asset.IsDir() {
			continue
		}
		mAsset, ok := m.assets[asset.URL()]
		if !ok {
			return true
		}
		if !mAsset.ModTime().Equal(asset.ModTime()) {
			return true
		}
	}
	return false

}

// Watch checks resources in the background thread and calls callback if any modification, or calls error handler if error
func (m *Tracker) Watch(ctx context.Context, fs afs.Service, callback func(URL string, operation Operation), onError func(err error)) {
	go m.watch(ctx, fs, callback, onError)
}

func (m *Tracker) watch(ctx context.Context, fs afs.Service, callback func(URL string, operation Operation), onError func(err error)) {
	for {
		err := m.Notify(ctx, fs, callback)
		if err != nil {
			onError(err)
		}
		time.Sleep(m.checkFrequency)
	}
}

// Notify returns true if resource under base URL have changed
func (m *Tracker) Notify(ctx context.Context, fs afs.Service, callback func(URL string, operation Operation)) error {
	if m.watchURL == "" {
		return nil
	}
	if !m.isCheckDue(time.Now()) {
		return nil
	}

	resources, err := fs.List(ctx, m.watchURL, option.NewRecursive(true))
	if err != nil {
		return errors.Wrapf(err, "failed to load rules %v", m.watchURL)
	}
	if !m.hasChanges(resources) {
		return nil
	}
	assets := NewAssets(resources)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.assets) == 0 {
		m.assets = make(map[string]storage.Object)
	}

	wg := sync.WaitGroup{}
	m.assets.Added(assets, func(object storage.Object) {
		wg.Add(1)
		go m.callInBackground(&wg, object.URL(), Added, callback)
	})
	m.assets.Modified(assets, func(object storage.Object) {
		wg.Add(1)
		go m.callInBackground(&wg, object.URL(), Modified, callback)
	})
	m.assets.Deleted(assets, func(object storage.Object) {
		wg.Add(1)
		go m.callInBackground(&wg, object.URL(), Deleted, callback)
	})
	wg.Wait()
	return nil
}

func (m *Tracker) callInBackground(wg *sync.WaitGroup, URL string, operation Operation, callback func(URL string, operation Operation)) {
	wg.Done()
	callback(URL, operation)
}

func New(watchURL string, checkFrequency time.Duration) *Tracker {
	if checkFrequency == 0 {
		checkFrequency = time.Minute
	}
	return &Tracker{
		checkFrequency: checkFrequency,
		mutex:          sync.Mutex{},
		watchURL:       watchURL,
		assets:         make(map[string]storage.Object),
	}
}
