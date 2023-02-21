package lambda

import (
	"net"
	"strconv"
)

func isPortAvailable(candidate int) bool {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(candidate))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}
