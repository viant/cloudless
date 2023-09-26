package sync

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgryski/go-farm"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"io"
	"reflect"
	"sort"
)

type Service struct {
	fs afs.Service
	checksums
}

func (s *Service) Sync(ctx context.Context, sync *Synchronization) error {
	reader, err := s.fs.OpenURL(ctx, sync.URL())
	if err != nil {
		return fmt.Errorf("failed to open %v, %w", sync.URL(), err)
	}
	defer reader.Close()

	if sync.Type == nil {
		sync.Type = reflect.TypeOf(sync.Provider())
	}
	var dataReader io.ReadCloser = reader
	if sync.Compressed {
		dataReader, err = gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("failed to open gzip reader: %v", sync.URL())
		}
		defer dataReader.Close()
	}
	lineReader := bufio.NewReader(dataReader)
	buffer := new(bytes.Buffer)
	var item interface{}
	var ok bool
	keyer := sync.Keyer
	isKeyer := keyer != nil && sync.Snapshoter != nil
	if isKeyer {
		if sync.checksum = s.checksums.get(sync.URL()); sync.checksum == nil {
			sync.checksum = newChecksum(20)
			s.checksums.put(sync.URL(), sync.checksum)
		}
	}

	nextChecksum := newChecksum(sync.checksum.size())
	for i := 0; ; i++ {
		buffer.Reset()
	readLine:
		line, isPrefix, err := lineReader.ReadLine()
		buffer.Write(line)
		if err != nil {
			break
		}
		if isPrefix {
			goto readLine
		}
		rawLine := buffer.Bytes()
		if len(rawLine) == 0 {
			continue
		}

		if isKeyer {

			if ok, err = s.reuseItemWithChecksum(rawLine, nextChecksum, sync); err != nil {
				return err
			}
			if ok {
				continue
			}
		}

		item = sync.Provider()
		if ok, err = s.unmarshalLine(sync, rawLine, item, i); err != nil {
			return err
		}
		if !ok {
			continue //in case CSV first line is skipped, thus there
		}
		ok, err = sync.Handler(item)
		if !ok || err != nil {
			return err
		}
	}

	if len(nextChecksum.intKeys) > 0 {
		sort.Sort(&intChecksum{nextChecksum})
	} else {
		sort.Sort(&stringsChecksum{nextChecksum})
	}
	s.checksums.put(sync.URL(), nextChecksum)
	return nil
}

func (s *Service) unmarshalLine(sync *Synchronization, rawLine []byte, item interface{}, lineIndex int) (bool, error) {
	if textUnmarshaler, ok := item.(CSVUnmarshaler); ok {
		if lineIndex == 0 && textUnmarshaler.SkipHeader() {
			return false, nil
		}
		if err := textUnmarshaler.UnmarshalCSV(string(rawLine)); err != nil {
			return false, fmt.Errorf("failed unmarshal CSV line: %v %v %w into %T\n", sync.URL(), lineIndex, err, item)
		}
		return true, nil
	}
	if _, ok := item.(gojay.UnmarshalerJSONObject); ok {
		if err := gojay.Unmarshal(rawLine, item); err != nil {
			return false, fmt.Errorf("failed unmarshal JSON  %v line: %v, %w", sync.URL(), lineIndex, err)
		}
		return true, nil
	}
	if err := json.Unmarshal(rawLine, item); err != nil {
		return false, fmt.Errorf("failed unmarshal JSON  %v line: %v, %w", sync.URL(), lineIndex, err)
	}
	return true, nil
}

func (s *Service) reuseItemWithChecksum(rawLine []byte, nextChecksum *checksum, sync *Synchronization) (bool, error) {
	key, err := sync.Keyer(rawLine)
	if err != nil {
		return false, err
	}
	next := Hash(rawLine)
	if err = nextChecksum.put(key, next); err != nil {
		return false, err
	}

	if previous, ok := sync.checksum.get(key); ok && next == previous {
		itemValue := reflect.New(sync.Type) //**T
		item := itemValue.Interface()
		if sync.Snapshoter(key, item) {
			//.Elem() changes **T to *T
			if ok, err := sync.Handler(itemValue.Elem().Interface()); !ok || err != nil {
				return ok, err
			}
			return true, nil
		}
	}
	return false, nil
}

// New creates a sync service
func New() *Service {
	return &Service{
		fs: afs.New(),
		checksums: checksums{
			asset: make(map[string]*checksum),
		},
	}
}

func Hash(data []byte) complex128 {
	l, h := farm.Hash128(data)
	return complex(float64(l), float64(h))
}
