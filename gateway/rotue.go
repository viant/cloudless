package gateway

type (
	Route struct {
		URI       string
		URIParams []string
		Resource  *Resource
		Security  *Security
	}

	Resource struct {
		URL string
	}

	Security struct {
		Authorizer string
	}
)
