package sync

import (
	"github.com/viant/cloudless/resource"
	"reflect"
)

type Synchronization struct {
	*resource.Asset
	Provider Provider
	Handler  Handler
	Keyer    Keyer
	Type     reflect.Type
	Snapshoter
	Checksumer Checksumer
	checksum   *checksum
}
