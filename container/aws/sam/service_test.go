package sam

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"io/ioutil"
	"net/http"
	"path"
	"testing"
	"time"
)

func TestNewTemplateWithURL(t *testing.T) {

	baseDir := toolbox.CallerDirectory(3)
	var testCases = []struct {
		description string
		templateURL string
	}{
		{
			description: "api template",
			templateURL: path.Join(baseDir, "testdata/template.yml"),
		},
	}

	for _, testCase := range testCases {
		tmpl, err := NewTemplateWithURL(context.Background(), testCase.templateURL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		if !assert.NotNilf(t, tmpl, testCase.description) {
			continue
		}
		srv, err := New(tmpl, &Config{})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		go srv.Start()
		time.Sleep(5 * time.Second)

		request, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:8081/mylambda/features/1", nil)
		request.Header.Set("Test-Key", "sid")
		respo, err := http.DefaultClient.Do(request)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		assert.Equal(t, 200, respo.StatusCode)
		_, err = ioutil.ReadAll(respo.Body)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
	}

}
