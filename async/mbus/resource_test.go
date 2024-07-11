package mbus

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/scy"
	"testing"
)

func TestEncodedResource_Decode(t *testing.T) {
	var testCases = []struct {
		description string
		encoded     string
		expected    *Resource
	}{
		{
			description: "valid resource",
			encoded:     "s3_queue;aws;queue;s3_queue;us-west-1;~/.secret/aws-e2e.json",
			expected: &Resource{
				Name:   "s3_queue",
				Vendor: "aws",
				Type:   ResourceTypeQueue,
				Region: "us-west-1",
				URL:    "s3_queue",
				Credentials: &scy.Resource{
					URL: "~/.secret/aws-e2e.json",
				},
			},
		},
	}

	for _, testCase := range testCases {
		actual, err := EncodedResource(testCase.encoded).Decode()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.Equal(t, testCase.expected, actual, testCase.description)
	}
}

/*
Name        string
	Region      string
	Vendor      string
	URL         string
	Credentials *scy.Resource
	Type        string      `description:"resource type: topic, subscription"`
	Client      interface{} `description:"client"`
*/
