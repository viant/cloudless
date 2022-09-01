package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/cloudless/resource"
	"github.com/viant/toolbox"
	"strings"
	"testing"
	"time"
)

func TestService_Sync(t *testing.T) {

	type Foo struct {
		ID     int
		Active bool
		Status int
	}
	var foos *[]*Foo
	snapshot := map[int]*Foo{}

	var testCases = []struct {
		sync        *Synchronization
		description string
		prev        string
		next        string
		expect      string
	}{
		{
			description: "unorder sync",
			prev: `{"id":4, "active":true, "status":2}
{"id":2, "active":true,"status":2}
{"id":3, "active":true,"status":2}
{"id":1, "active":true,"status":2}`,
			next: `{"id":4, "active":true, "status":2}
{"id":2, "active":true,"status":2}
{"id":3, "active":false,"status":2}
{"id":1, "active":true,"status":3}`,
			sync: &Synchronization{
				Asset: &resource.Asset{
					SourceURL:      "mem://localhost/temp/foo.json",
					CheckFrequency: 5 * time.Millisecond,
				},
				Provider: func() interface{} {
					return &Foo{}
				},
				Snapshoter: func(key interface{}, targetPtr interface{}) bool {
					prev, ok := snapshot[toolbox.AsInt(key)]
					if !ok {
						return ok
					}
					target, ok := targetPtr.(**Foo)
					if !ok {
						return false
					}
					*target = prev
					return ok
				},
				Handler: func(target interface{}) (bool, error) {
					foo, ok := target.(*Foo)
					if !ok {
						return false, fmt.Errorf("expected %T, but had: %T", foo, target)
					}
					if foos == nil {
						aSlice := make([]*Foo, 0)
						foos = &aSlice
					}
					*foos = append(*foos, foo)
					return true, nil
				},
				Keyer: IntKeyJSONExtractor("id"),
			},
			expect: `[{"ID":4,"Active":true,"Status":2},{"ID":2,"Active":true,"Status":2},{"ID":3,"Active":false,"Status":2},{"ID":1,"Active":true,"Status":3}]`,
		},
	}

	for _, testCase := range testCases {
		srv := New()
		fs := afs.New()
		err := fs.Upload(context.Background(), testCase.sync.SourceURL, file.DefaultFileOsMode, strings.NewReader(testCase.prev))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		err = srv.Sync(context.Background(), testCase.sync)
		assert.Nil(t, err, testCase.description)
		time.Sleep(testCase.sync.CheckFrequency)
		err = fs.Upload(context.Background(), testCase.sync.SourceURL, file.DefaultFileOsMode, strings.NewReader(testCase.next))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for i, item := range *foos {
			snapshot[item.ID] = (*foos)[i]
		}
		foos = nil
		err = srv.Sync(context.Background(), testCase.sync)
		actual, _ := json.Marshal(*foos)
		assert.Equal(t, testCase.expect, string(actual), testCase.description)

	}
}
