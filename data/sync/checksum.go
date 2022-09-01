package sync

import (
	"fmt"
	"sort"
	"sync"
)

type (
	//Checksumer returns a record checksum
	Checksumer func([]byte) complex128

	checksum struct {
		intKeys []int
		keys    []string
		hash    []complex128
	}

	intChecksum struct {
		*checksum
	}
	stringsChecksum struct {
		*checksum
	}

	checksums struct {
		asset map[string]*checksum
		mux   sync.RWMutex
	}
)

func (c *checksum) get(key interface{}) (complex128, bool) {
	switch k := key.(type) {
	case int:
		index := sort.SearchInts(c.intKeys, k)
		if index == len(c.intKeys) || index == -1 {
			return 0, false
		}
		return c.hash[index], true
	case string:
		index := sort.SearchStrings(c.keys, k)
		if index == len(c.keys) || index == -1 {
			return 0, false
		}
		return c.hash[index], true
	default:
		return 0, false
	}
}

func (c *checksum) put(key interface{}, checksum complex128) error {
	switch k := key.(type) {
	case int:
		c.intKeys = append(c.intKeys, k)
	case string:
		c.keys = append(c.keys, k)
	default:
		return fmt.Errorf("unsupported key: %T", key)
	}
	c.hash = append(c.hash, checksum)
	return nil
}

func (c *checksum) size() int {
	if c == nil {
		return 0
	}
	return len(c.hash)
}

func newChecksum(cap int) *checksum {
	return &checksum{hash: make([]complex128, 0, int(1.2*float64(cap)))}
}

func newChecksums() *checksums {
	return &checksums{asset: map[string]*checksum{}}
}

func (c *checksums) get(URL string) *checksum {
	c.mux.RLock()
	result, _ := c.asset[URL]
	c.mux.RUnlock()
	return result
}

func (c *checksums) put(URL string, checksum *checksum) {
	c.mux.Lock()
	c.asset[URL] = checksum
	c.mux.Unlock()
}

// Len is part of sort.Interface.
func (s *intChecksum) Len() int {
	return len(s.intKeys)
}

// Swap is part of sort.Interface.
func (s *intChecksum) Swap(i, j int) {
	s.intKeys[i], s.intKeys[j] = s.intKeys[j], s.intKeys[i]
	s.hash[i], s.hash[j] = s.hash[j], s.hash[i]

}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *intChecksum) Less(i, j int) bool {
	return s.intKeys[i] < s.intKeys[j]
}

// Len is part of sort.Interface.
func (s *stringsChecksum) Len() int {
	return len(s.intKeys)
}

// Swap is part of sort.Interface.
func (s *stringsChecksum) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.hash[i], s.hash[j] = s.hash[j], s.hash[i]

}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *stringsChecksum) Less(i, j int) bool {
	return s.intKeys[i] < s.intKeys[j]
}
