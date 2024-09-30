package mbus

import (
	"fmt"
	"github.com/viant/scy"
	"strings"
	"sync"
)

const (
	ResourceTypeTopic        = "topic"
	ResourceTypeSubscription = "subscription"
	ResourceTypeQueue        = "queue"
)

type Resource struct {
	ID          string        `yaml:"ID" json:",omitempty"`
	Name        string        `yaml:"Name"  json:",omitempty"`
	Region      string        `yaml:"Region"  json:",omitempty"`
	Vendor      string        `yaml:"Vendor"  json:",omitempty"`
	URL         string        `yaml:"URL"  json:",omitempty"`
	Credentials *scy.Resource `yaml:"Resource"  json:",omitempty"`
	Type        string        `description:"resource type: topic, subscription" yaml:"Type"  json:",omitempty"`
	Client      interface{}   `description:"client" yaml:"-"`
	sync.Mutex  `yaml:"-" json:"-"`
}

func (r *Resource) Init() error {
	if r == nil {
		return fmt.Errorf("resource was empty")
	}
	if r.URL != "" {
		if r.Name == "" {
			r.Name = r.URL
			index := strings.LastIndex(r.URL, "/")
			if index == -1 {
				index = strings.LastIndex(r.URL, ":")
			}
			if index != -1 {
				r.Name = r.URL[index+1:]
			}
		}
	}
	return nil
}

// EncodedResource represents encoded resource
type EncodedResource string

func (e EncodedResource) Decode() (*Resource, error) {
	ret := &Resource{}
	var parts []string
	if strings.Contains(string(e), "|") {
		parts = strings.Split(string(e), "|")
	} else {
		parts = strings.Split(string(e), ";")
	}
	partLen := len(parts)
	if partLen < 5 {
		return nil, fmt.Errorf("faield to decode mbus resource: invalid format: %v, expected:id|name|vendor|resourceType|uri[|region|secretURL|secretKey]", e)
	}
	ret.ID = parts[0]
	ret.Name = parts[1]
	ret.Vendor = parts[2]
	ret.Type = parts[3]
	switch ret.Type {
	case ResourceTypeTopic, ResourceTypeQueue, ResourceTypeSubscription:
	default:
		return nil, fmt.Errorf("invalid resource: type: %v, expected:%v", ret.Type, []string{ResourceTypeTopic, ResourceTypeQueue, ResourceTypeSubscription})
	}
	ret.URL = parts[4]
	if partLen > 5 {
		ret.Region = parts[5]
	}

	if partLen > 6 {
		ret.Credentials = &scy.Resource{
			URL: parts[5],
		}
		if partLen > 6 {
			ret.Credentials.Key = parts[7]
		}
	}
	return ret, nil
}
