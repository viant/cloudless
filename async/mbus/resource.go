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
