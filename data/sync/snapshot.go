package sync

//Snapshoter returns previous value into target key and return true, otherwise false
type Snapshoter func(key interface{}, targetPtr interface{}) bool
