package mbus

import "sync"

type registry struct {
	services  map[string]Service
	notifiers map[string]Notifier
	sync.RWMutex
}

// Lookup returns register message service
func (r *registry) Lookup(vendor string) Service {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	return r.services[vendor]
}

// Register register message service
func (r *registry) Register(vendor string, service Service) {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.services[vendor] = service
}

// RegisterNotifier register notifer service
func (r *registry) RegisterNotifier(vendor string, service Notifier) {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.notifiers[vendor] = service
}

func (r *registry) LookupNotifier(vendor string) Notifier {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	return r.notifiers[vendor]
}

var r = registry{services: map[string]Service{}, notifiers: map[string]Notifier{}}

// Register register vendor service
func Register(vendor string, service Service) {
	r.Register(vendor, service)
}

// RegisterNotifier register vendor notifier
func RegisterNotifier(vendor string, notifier Notifier) {
	r.RegisterNotifier(vendor, notifier)
}

// Lookup lookup service
func Lookup(vendor string) Service {
	return r.Lookup(vendor)
}

// LookupNotifier lookup notifier
func LookupNotifier(vendor string) Notifier {
	return r.LookupNotifier(vendor)
}
