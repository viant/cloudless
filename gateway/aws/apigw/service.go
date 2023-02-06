package apigw

import (
	"context"
	"encoding/json"
	"fmt"
	//"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/viant/cloudless/gateway"
	"net/http"
)

type Service struct {
	cfg    *Config
	client *lambda.Lambda
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

	/*
		match route
		if route has autorizer call lambda it (extract Auhentication header as token to authoizer)
		-> handle response if error not nill return
		-> otherwise copy events.APIGatewayCustomAuthorizerResponse.Context to APiProxyRequest.APIGatewayProxyRequestContext.Authorizer map
	*/

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
		return nil, fmt.Errorf("%w", *output.FunctionError)
	}
	return output.Payload, err
}
