package mbus

import "sync"

type registry struct {
	registry map[string]Service
	sync.RWMutex
}

//Lookup returns register message service
func (r *registry) Lookup(vendor string) Service {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	return r.registry[vendor]
}

//Register register message service
func (r *registry) Register(vendor string, service Service) {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.registry[vendor] = service
}

var r = registry{registry: map[string]Service{}}

//Register register vendor service
func Register(vendor string, service Service) {
	r.Register(vendor, service)
}

//Lookup lookup
func Lookup(vendor string) Service {
	return r.Lookup(vendor)
}
