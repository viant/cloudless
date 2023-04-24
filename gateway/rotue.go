package gateway

type (
	Authorizers []*Resource

	Routes []*Route

	Route struct {
		URI        string
		HTTPMethod string
		URIParams  []string
		Resource   *Resource
		Security   *Security
	}

	Resource struct {
		URL  string
		Name string
	}

	Security struct {
		Authorizer string
	}
)
