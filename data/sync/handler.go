package sync

type Handler func(target interface{}) (bool, error)
