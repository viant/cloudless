package sync

//Keyer returns an item key from byte steam
type Keyer func(encoded []byte) (interface{}, error)
