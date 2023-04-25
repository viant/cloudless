package lambda

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"path"
	"testing"
	"time"
)

func TestNew(t *testing.T) {

	baseDir := toolbox.CallerDirectory(3)
	var testCases = []struct {
		description string
		fnLocation  string
		logLocation string

		port int
		opts []ConfigOption
	}{
		{
			fnLocation: path.Join(baseDir, "testdata/task/"),
			opts: []ConfigOption{
				WithFunction(NewFunctionConfig("test", "darwin_amd64_handler")),
			},
		},
	}

	for _, testCase := range testCases {
		cfg := NewConfig(testCase.port, testCase.fnLocation, testCase.logLocation, testCase.opts...)
		srv := New(cfg)
		srv.Start()
		time.Sleep(100 * time.Millisecond)
		fn, err := srv.Function("test")
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		assert.NotNilf(t, fn, testCase.description)

		lambdaClient, err := LocalClient(&aws.Config{Region: aws.String("us-west-1")})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		for j := 0; j < 10; j++ {
			_, err := lambdaClient.Invoke(&lambda.InvokeInput{
				FunctionName:  aws.String("test"),
				ClientContext: aws.String(`{"Secret":1}`),
				Payload:       []byte(`{"k":1}`)})
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			//	fmt.Printf("output: %s", output.Payload)
		}
		srv.Shutdown()
	}
}

func LocalClient(config *aws.Config) (*lambda.Lambda, error) {
	var options = append([]*aws.Config{}, &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://127.0.0.1:9001")})
	sess, err := session.NewSession(options...)
	if err != nil {
		return nil, err
	}
	return lambda.New(sess, config), nil
}
