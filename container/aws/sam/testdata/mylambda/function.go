package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"net/http"
	"os"
)

func main() {
	lambda.Start(handleRequest)
}

type Response struct {
	Status string
	Data   []*DummyRecord
}
type DummyRecord struct {
	ID   int
	Name string
}

func handleRequest(ctx context.Context, apiRequest events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	fmt.Printf("%v %v \nBody:%v \nisBase64Encoded:%v\n", apiRequest.HTTPMethod, apiRequest.Path, apiRequest.Body, apiRequest.IsBase64Encoded)
	logStruct("path parameters: %s\n", apiRequest.PathParameters)
	var env = map[string]string{}
	for _, k := range os.Environ() {
		env[k] = os.Getenv(k)
	}
	logStruct("env: %s\n", env)
	appResponse := &Response{Status: "ok", Data: []*DummyRecord{
		{
			ID:   1,
			Name: "Name 1",
		},
		{
			ID:   2,
			Name: "Name 2",
		},
	}}
	responseJSON, _ := json.Marshal(appResponse)
	apiResponse := events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       string(responseJSON),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
	setCORSHeaderIfNeeded(&apiRequest, &apiResponse)
	return apiResponse, nil
}

func logStruct(tempalte string, s interface{}) {
	data, _ := json.Marshal(s)
	fmt.Printf(tempalte, string(data))
}
func setCORSHeaderIfNeeded(apiRequest *events.APIGatewayProxyRequest, response *events.APIGatewayProxyResponse) {
	origin, ok := apiRequest.Headers["Origin"]
	if !ok {
		return
	}
	if len(response.Headers) == 0 {
		response.Headers = make(map[string]string)
	}
	response.Headers["Access-Control-Allow-Credentials"] = "true"
	response.Headers["Access-Control-Allow-Origin"] = origin
	response.Headers["Access-Control-Allow-Methods"] = "POST GET"
	response.Headers["Access-Control-Allow-Headers"] = "Content-Type, *"
	response.Headers["Access-Control-Max-Age"] = "120"
}
