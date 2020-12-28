package ioutil

import (
	"context"
	"github.com/viant/afs/storage"
	"net"
	"net/http"
	"time"
)

//NewHttpClientProvider crates custom HTTP client provider overcoming Cloud Function default DNS restriction.
func NewHttpClientProvider(concurrency int, timeout time.Duration, dnsResolver string) func(baseURL string, options ...storage.Option) (*http.Client, error) {
	return func(baseURL string, options ...storage.Option) (*http.Client, error) {
		transport := &http.Transport{
			DisableKeepAlives: false,
			IdleConnTimeout:   time.Second,
			MaxIdleConns:      concurrency,
		}
		transport.DialContext = (&net.Dialer{
			Resolver: &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					d := net.Dialer{}
					return d.DialContext(ctx, "udp", dnsResolver)
				},
			},
		}).DialContext
		return &http.Client{
			Transport: transport,
			Timeout:   timeout,
		}, nil
	}
}
