package lambda

import (
	"fmt"
	"net/http"
)

type Router struct{}

func (r *Router) Configure(service *Service) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/2015-03-31/functions/", service.Handle)
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Printf("unsupported %v %v\n", request.Method, request.RequestURI)
	})
	return mux
}
