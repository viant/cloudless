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
		if index == len(c.intKeys) || c.intKeys[index] != k {
			return 0, false
		}
		return c.hash[index], true
	case string:
		index := sort.SearchStrings(c.keys, k)
		if index == len(c.keys) || c.keys[index] != k {
			return 0, false
		}
		return c.hash[index], true
	default:
		return 0, false
	}
}

func (c *checksum) put(key interface{}, checksum complex128) error {
	var index int
	switch k := key.(type) {
	case int:
		c.intKeys, index = insertInt(c.intKeys, k)
	case string:
		c.keys, index = insertString(c.keys, k)
	default:
		return fmt.Errorf("unsupported key: %T", key)
	}
	c.hash = insertComplex128At(c.hash, index, checksum)
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

func insertInt(data []int, v int) ([]int, int) {
	if l := len(data); l == 0 || data[l-1] <= v {
		return append(data, v), l
	}
	i := sort.SearchInts(data, v)
	data = append(data[:i+1], data[i:]...)
	data[i] = v
	return data, i
}

func insertString(data []string, v string) ([]string, int) {
	if l := len(data); l == 0 || data[l-1] <= v {
		return append(data, v), l
	}
	i := sort.SearchStrings(data, v)
	data = append(data[:i+1], data[i:]...)
	data[i] = v
	return data, i
}

func insertComplex128At(data []complex128, i int, v complex128) []complex128 {
	if i == len(data) {
		return append(data, v)
	}
	data = append(data[:i+1], data[i:]...)
	data[i] = v
	return data
}
