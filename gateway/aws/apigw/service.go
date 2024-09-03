package apigw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/viant/cloudless/gateway"
	apigwHttp "github.com/viant/cloudless/gateway/aws/apigw/http"
	"io"
	"net/http"
	"strings"
)

type Service struct {
	cfg    *Config
	client *lambda.Lambda
	router *Router
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

	statucCode, err := s.do(ctx, request, writer)
	if err != nil {
		http.Error(writer, err.Error(), statucCode)
	}
	//_, _ = writer.write(output)
}

func (s *Service) do(ctx context.Context, request *http.Request, writer http.ResponseWriter) (int, error) {
	route, err := s.router.FindRoute(request)
	if err != nil {
		return http.StatusNotFound, err
	}

	authorizer := map[string]interface{}{}
	req := apigwHttp.Request(*request)

	if route.Security != nil {
		auth := request.Header.Get("Authorization")
		if auth == "" {
			return 401, err
		}

		authSegments := strings.Split(auth, " ")
		if len(authSegments) != 2 {
			return http.StatusBadRequest, errors.New("incorrect Authorization header format")
		}

		lambdaOutput, err := s.CallLambda(ctx, route.Resource, req.AuthorizerRequest())
		if err != nil {
			return http.StatusInternalServerError, err
		}

		if err = json.Unmarshal(lambdaOutput, &authorizer); err != nil {
			return http.StatusInternalServerError, err
		}
	}

	apiGwRequest, err := req.ProxyRequest(route, authorizer, request.Body)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	output, err := s.CallLambda(ctx, route.Resource, apiGwRequest)
	if err != nil {
		return http.StatusBadRequest, err
	}

	apiGwResponse := &events.APIGatewayProxyResponse{}
	if err = json.Unmarshal(output, apiGwResponse); err != nil {
		return http.StatusInternalServerError, err
	}

	httpResponse, err := apigwHttp.NewResponse(apiGwResponse)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	s.update(httpResponse, writer)
	return 0, nil
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

func (s *Service) update(response *http.Response, writer http.ResponseWriter) {
	writer.WriteHeader(response.StatusCode)
	if len(response.Header) > 0 {
		for k, vals := range response.Header {
			for _, v := range vals {
				writer.Header().Add(k, v)
			}
		}
	}
	if response.Body != nil {
		io.Copy(writer, response.Body)
		response.Body.Close()
	}
}

func New(router *Router, cfg *Config) *Service {
	return &Service{router: router, cfg: cfg}
}
