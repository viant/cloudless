package resource

type Operation int

const (
	Undefined = -1
	Added     = Operation(iota)
	Modified
	Deleted
)
