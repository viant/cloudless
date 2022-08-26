package row_type

import "reflect"

var typeRegistry = map[string]reflect.Type{}

func Register(name string, rowType reflect.Type) {
	typeRegistry[name] = rowType
}

func RowType(name string) reflect.Type {
	return typeRegistry[name]
}
