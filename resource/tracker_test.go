package resource

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/file"
	"log"
	"testing"
	"time"
)

func TestTracker_HasChanged(t *testing.T) {

	var useCases = []struct {
		description    string
		baseURL        string
		fsAtT0         []*asset.Resource
		fsAtT1         []*asset.Resource
		expected       map[string]Operation
		checkFrequency time.Duration
		sleepDuration  time.Duration
		recreateFsAtT1 bool
	}{
		{
			description: "test addition url ",
			baseURL:     "mem://localhost/case1",
			fsAtT0: []*asset.Resource{
				asset.NewFile("abc.json", []byte("foo bar"), file.DefaultFileOsMode),
			},
			fsAtT1: []*asset.Resource{
				asset.NewFile("def.json", []byte("car sar"), file.DefaultFileOsMode),
			},

			expected: map[string]Operation{
				"mem://localhost/case1/def.json": OperationAdded,
			},

			checkFrequency: 100 * time.Millisecond,
			sleepDuration:  200 * time.Millisecond,
		},
		{
			description: "test modification url ",
			baseURL:     "mem://localhost/case2",
			fsAtT0: []*asset.Resource{
				asset.NewFile("abc.json", []byte("foo1 bar1"), file.DefaultFileOsMode),
			},
			fsAtT1: []*asset.Resource{
				asset.NewFile("abc.json", []byte("foo12 sar12"), file.DefaultFileOsMode),
			},

			expected: map[string]Operation{
				"mem://localhost/case2/abc.json": OperationModified,
			},

			checkFrequency: 100 * time.Millisecond,
			sleepDuration:  200 * time.Millisecond,
		},
		{
			description: "test deletion url ",
			baseURL:     "mem://localhost/case3",
			fsAtT0: []*asset.Resource{
				asset.NewFile("abcd.json", []byte("foo1 bar1"), file.DefaultFileOsMode),
			},
			fsAtT1: []*asset.Resource{
				asset.NewFile("abcdef.json", []byte("foo123 sar123"), file.DefaultFileOsMode),
			},
			recreateFsAtT1: true,
			expected: map[string]Operation{
				"mem://localhost/case3/abcd.json":   OperationDeleted,
				"mem://localhost/case3/abcdef.json": OperationAdded,
			},

			checkFrequency: 100 * time.Millisecond,
			sleepDuration:  200 * time.Millisecond,
		},
		{
			description: "test undetected change due to frequency check ",
			baseURL:     "mem://localhost/case4",
			fsAtT0: []*asset.Resource{
				asset.NewFile("abcd.json", []byte("foo1 bar1"), file.DefaultFileOsMode),
			},
			fsAtT1: []*asset.Resource{
				asset.NewFile("abcdef.json", []byte("foo123 sar123"), file.DefaultFileOsMode),
			},
			recreateFsAtT1: true,
			expected:       map[string]Operation{},

			checkFrequency: 200 * time.Millisecond,
			sleepDuration:  10 * time.Millisecond,
		},
	}
	ctx := context.Background()
	fs := afs.New()
	for _, useCase := range useCases {
		mgr, err := afs.Manager(useCase.baseURL)
		if err != nil {
			log.Fatal(err)
		}
		err = asset.Create(mgr, useCase.baseURL, useCase.fsAtT0)
		if err != nil {
			log.Fatal(err)
		}
		tracker := New(useCase.baseURL, useCase.checkFrequency)
		initialResourcesCount := 0
		err = tracker.Notify(ctx, fs, func(URL string, operation Operation) {
			initialResourcesCount++
		})
		assert.Nil(t, err, useCase.description)
		assert.Equal(t, len(useCase.fsAtT0), initialResourcesCount, useCase.description)

		var updateFS = asset.Modify
		if useCase.recreateFsAtT1 {
			updateFS = asset.Create
		}
		err = updateFS(mgr, useCase.baseURL, useCase.fsAtT1)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(useCase.sleepDuration)
		actual := make(map[string]Operation)
		err = tracker.Notify(ctx, fs, func(URL string, operation Operation) {
			actual[URL] = operation
		})

		assert.EqualValues(t, useCase.expected, actual, useCase.description)

	}
}
