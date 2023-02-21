package apigw

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/viant/cloudless/gateway"
	apigwHttp "github.com/viant/cloudless/gateway/aws/apigw/http"
	"net/http"
)

type Service struct {
	cfg       *Config
	client    *lambda.Lambda
	converter *Router
}

func (s *Service) ensureClient() error {
	if s.client != nil {
		return nil
	}
	sess, err := s.newSession()
	if err != nil {
		return err
	}
	s.client = lambda.New(sess, s.cfg.AWS)
	return nil
}

func (s *Service) newSession() (*session.Session, error) {
	var options []*aws.Config
	if s.cfg.Endpoint != "" {
		options = append(options, &aws.Config{
			Region:   aws.String(s.cfg.Region),
			Endpoint: aws.String(s.cfg.Endpoint)})
	}
	return session.NewSession(options...)
}

func (s *Service) Do(writer http.ResponseWriter, request *http.Request) {
	ctx := context.Background()

	output, statusCode := s.do(ctx, request)
	writer.WriteHeader(statusCode)
	_, _ = writer.Write(output)
}

func (s *Service) do(ctx context.Context, request *http.Request) ([]byte, int) {
	route, err := s.converter.FindRoute(request)
	if err != nil {
		return []byte(err.Error()), http.StatusNotFound
	}

	authorizer := map[string]interface{}{}
	req := apigwHttp.Request(*request)

	if route.Security != nil {
		auth := request.Header.Get("Authorization")
		if auth == "" {
			return []byte(""), http.StatusUnauthorized
		}

		authSegments := strings.Split(auth, " ")
		if len(authSegments) != 2 {
			return []byte("incorrect Authorization header format"), http.StatusBadRequest
		}

		lambdaOutput, err := s.CallLambda(ctx, route.Resource, req.AuthorizerRequest())
		if err != nil {
			return []byte(err.Error()), http.StatusInternalServerError
		}

		if err = json.Unmarshal(lambdaOutput, &authorizer); err != nil {
			return []byte(err.Error()), http.StatusInternalServerError
		}
	}

	actualRequest := req.ProxyRequest(route, authorizer)

	output, err := s.CallLambda(ctx, route.Resource, actualRequest)
	if err != nil {
		return output, http.StatusBadRequest
	}

	return output, http.StatusOK
}

func (s *Service) CallLambda(ctx context.Context, route *gateway.Resource, anEvent interface{}) ([]byte, error) {
	if err := s.ensureClient(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(anEvent)
	if err != nil {
		return nil, err
	}
	input := &lambda.InvokeInput{
		FunctionName: &route.Name,
		Payload:      payload,
	}
	output, err := s.client.InvokeWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	if output.FunctionError != nil {
		return nil, fmt.Errorf("%v", *output.FunctionError)
	}
	return output.Payload, err
}
