package stat


type Values []interface{}

func (v *Values) Append(item interface{}) {
	*v = append(*v, item)
}

func (v *Values) Values() []interface{} {
	return *v
}

//NewValues creates a values slice
func NewValues() *Values {
	return &Values{}
}
