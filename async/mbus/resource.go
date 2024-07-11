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
	Name        string
	Region      string
	Vendor      string
	URL         string
	Credentials *scy.Resource
	Type        string      `description:"resource type: topic, subscription"`
	Client      interface{} `description:"client"`
	sync.Mutex
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
	parts := strings.Split(string(e), ";")
	partLen := len(parts)
	if partLen < 4 {
		return nil, fmt.Errorf("faield to decode mbus resource: invalid format: %v, expected:name;vendor;resourceType;uri[;region;secretURL;secretKey]", e)
	}
	ret.Name = parts[0]
	ret.Vendor = parts[1]
	ret.Type = parts[2]
	switch ret.Type {
	case ResourceTypeTopic, ResourceTypeQueue, ResourceTypeSubscription:
	default:
		return nil, fmt.Errorf("invalid resource: type: %v, expected:%v", ret.Type, []string{ResourceTypeTopic, ResourceTypeQueue, ResourceTypeSubscription})
	}
	ret.URL = parts[3]
	if partLen > 4 {
		ret.Region = parts[4]
	}

	if partLen > 5 {
		ret.Credentials = &scy.Resource{
			URL: parts[5],
		}
		if partLen > 6 {
			ret.Credentials.Key = parts[6]
		}
	}
	return ret, nil
}
