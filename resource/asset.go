package resource

import "github.com/viant/afs/storage"

//Assets represents storage assets
type Assets map[string]storage.Object

//Deleted calls a callback with deleted assets
func (a Assets) Deleted(assets Assets, fn func(object storage.Object)) {
	for URL, asset := range a {
		if _, ok := assets[URL]; !ok {
			fn(asset)
			delete(a, URL)
		}
	}
}

//Modified calls a callback with modified assets
func (a Assets) Modified(assets Assets, fn func(object storage.Object)) {
	for URL, asset := range assets {
		if prev, ok := a[URL]; ok {
			if prev.ModTime() != asset.ModTime() {
				fn(asset)
				a[URL] = assets[URL]
			}
		}
	}
}

//Added calls a callback with added assets
func (a Assets) Added(assets Assets, fn func(object storage.Object)) {
	for URL, asset := range assets {
		if _, ok := a[URL]; !ok {
			fn(asset)
			a[URL] = assets[URL]
		}
	}
}

func NewAssets(assets []storage.Object) Assets {
	var result = make(map[string]storage.Object)
	for i, asset := range assets {
		if asset.IsDir() {
			continue
		}
		result[asset.URL()] = assets[i]
	}
	return result
}
