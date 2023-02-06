package gateway

type (
	Authorizers []*Resource

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
