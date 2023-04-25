package sam

import (
	"context"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"path"
	"strings"
)

type (
	Template struct {
		Resources map[string]*Resource
		Globals   *Globals
	}

	Globals struct {
		Function *Function `json:",omitempty"`
	}

	Resource struct {
		Type       string     `json:",omitempty"`
		Function   *Function  `json:",omitempty"`
		Properties Properties `json:",omitempty"`
	}

	Function struct {
		lambda.FunctionConfiguration
		Authorizer string //name of lambda function that act as authorizer

	}

	Properties struct {
		CodeUri string `json:",omitempty"`
		Events  map[string]*Resource
		Path    string `json:",omitempty"`
		Method  string `json:",omitempty"`
		*Function
	}
)

func (p *Properties) CodeURL(baseURL string) string {
	if strings.HasPrefix(p.CodeUri, "/") || strings.Contains(p.CodeUri, "://") {
		return p.CodeUri
	}
	return path.Join(baseURL, p.CodeUri)
}

func (t *Template) Init() error {
	toolbox.DumpIndent(t, true)
	return nil
}

func NewTemplateWithURL(ctx context.Context, URL string) (*Template, error) {
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	any := map[string]interface{}{}
	if err = yaml.Unmarshal(data, &any); err != nil {
		return nil, err
	}
	template := &Template{}
	if err = toolbox.DefaultConverter.AssignConverted(template, any); err != nil {
		return nil, err
	}
	return template, template.Init()
}
