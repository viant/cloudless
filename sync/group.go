package sync

import (
	"context"
	"github.com/viant/afs"
)

type Group struct {
	fs afs.Service
	URL     string
	Counter int
}

func (g *Group) Increment(ctx context.Context) (int, error) {
	//TODO add implementation
	return 0, nil
}

func (g *Group) Decrement(ctx context.Context) (int, error) {
	//TODO add implementation
	return 0, nil
}
