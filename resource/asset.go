package resource

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"strings"
	"time"
)

// Asset represents a storage asset
type Asset struct {
	SourceURL      string
	Compressed     bool
	Source         storage.Object
	CheckFrequency time.Duration
	next           storage.Object
	nextCheck      time.Time
}

func (m *Asset) URL() string {
	if m.Compressed && strings.HasPrefix(m.SourceURL, ".gzip") {
		return m.SourceURL + ".gzip"
	}
	return m.SourceURL
}

func (m *Asset) IsCheckDue(now time.Time) bool {
	if m.nextCheck.IsZero() || now.After(m.nextCheck) {
		m.nextCheck = now.Add(m.CheckFrequency)
		return true
	}
	return false
}

func (m *Asset) HasChanged(ctx context.Context, fs afs.Service) (bool, error) {
	now := time.Now()
	if !m.IsCheckDue(now) {
		return false, nil
	}
	next, err := fs.Object(ctx, m.SourceURL)
	if err != nil {
		return false, err
	}
	m.next = next
	return next.ModTime().Equal(m.Source.ModTime()), nil
}

func (m *Asset) Sync() {
	m.Source = m.next
	m.next = nil
}
