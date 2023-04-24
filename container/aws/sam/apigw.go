package sam

import (
	"github.com/viant/cloudless/gateway"
)

//Routes returns gateway routes with lambda info
func (t *Template) Routes() (gateway.Routes, error) {
	var result gateway.Routes
	for name, res := range t.Resources {
		switch res.Type {
		case "AWS::Serverless::Function":
			if len(res.Properties.Events) == 0 {
				continue
			}
			for _, event := range res.Properties.Events {
				prop := event.Properties
				route := &gateway.Route{
					URI:        prop.Path,
					HTTPMethod: prop.Method,
					URIParams:  nil,
					Resource: &gateway.Resource{
						URL:  "http://127.0.0.1:9001",
						Name: name,
					},
				}
				if res.Function.Authorizer != "" {
					route.Security = &gateway.Security{Authorizer: res.Function.Authorizer}
				}
				result = append(result, route)
			}
		}
	}
	return result, nil
}
