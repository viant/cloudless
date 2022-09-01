package sync

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

//IntKeyJSONExtractor returns function returning int key value
func IntKeyJSONExtractor(key string) func(data []byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		match := `"` + key + `":`
		offset := bytes.Index(data, []byte(match))
		if offset == -1 {
			return nil, fmt.Errorf("failed to locate: %v", key)
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
		return strconv.Atoi(value)
	}
}

//StringKeyJSONExtractor returns function returning string key value
func StringKeyJSONExtractor(key string) func(data []byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		match := `"` + key + `":`
		offset := bytes.Index(data, []byte(match))
		if offset == -1 {
			return nil, fmt.Errorf("failed to locate: %v", key)
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
		return value, nil
	}
}
