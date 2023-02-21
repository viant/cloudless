package lambda

import "strings"

const (
	functionFragment = "/functions/"
)

type URI string

func (u URI) Info() (string, string) {
	index := strings.Index(string(u), functionFragment)
	if index == -1 {
		return "", ""
	}
	fragment := u[index+len(functionFragment):]
	pair := strings.SplitN(string(fragment), "/", 2)
	return pair[0], pair[1]
}
