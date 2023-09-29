package sync

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// IntKeyJSONExtractor returns function returning int key value
func IntKeyJSONExtractor(key string) func(data []byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		value, i, err := extractJSONKeyValue(data, key)
		if err != nil {
			return i, err
		}
		return strconv.Atoi(value)
	}
}

// StringKeyJSONExtractor returns function returning string key value
func StringKeyJSONExtractor(key string) func(data []byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		value, i, err := extractJSONKeyValue(data, key)
		value = strings.Trim(value, "\"")
		if err != nil {
			return i, err
		}
		return value, nil
	}
}

func extractJSONKeyValue(data []byte, key string) (string, interface{}, error) {
	match := `"` + key + `":`
	offset := bytes.Index(data, []byte(match))
	if offset == -1 {
		return "", nil, fmt.Errorf("failed to locate: %v", key)
	}
	var limit = 0
outer:
	for limit = offset + len(match); limit < len(data); limit++ {
		c := data[limit]
		switch c {
		case ',', '}':
			break outer
		}
	}
	value := strings.TrimSpace(string(data[offset+len(match) : limit]))
	return value, nil, nil
}

func CompositeKey(keys ...string) func(data []byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		values := make([]string, len(keys))
		n := 0
		for i := range keys {
			match := `"` + keys[i] + `":`
			indexAt(data, []byte(match), n)
			offset := indexAt(data, []byte(match), n)
			if offset == -1 {
				return nil, fmt.Errorf("failed to locate: %v", keys[i])
			}
			var limit = 0
		outer:
			for limit = offset + len(match); limit < len(data); limit++ {
				c := data[limit]
				switch c {
				case ',', '}':
					break outer
				}
			}
			values[i] = strings.Trim(strings.TrimSpace(string(data[offset+len(match):limit])), "\"")
			n += offset + len(match) + len(values[i])
		}
		return strings.Join(values, "/"), nil
	}
}

func indexAt(s, sep []byte, n int) int {
	idx := bytes.Index(s[n:], sep)
	if idx > -1 {
		idx += n
	}
	return idx
}
