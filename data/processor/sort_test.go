package processor

import (
	"github.com/stretchr/testify/assert"
	iou "io/ioutil"
	"strings"
	"testing"
)

func TestSort_Order(t *testing.T) {

	var useCases = []struct {
		description string
		sort        Sort
		input       string
		expect      string
	}{
		{
			description: "single CSV numeric order by",
			input: `zz,3,abc
cc,1,xyz
bb,2,kdl`,
			expect: `cc,1,xyz
bb,2,kdl
zz,3,abc`,
			sort: Sort{
				Spec: Spec{
					Format:    "CSV",
					Delimiter: ",",
				},
				By: []Field{
					{
						Index:     1,
						Name:      "foo",
						IsNumeric: true,
					},
				},
			},
		},
		{
			description: "single JSON numeric order by",
			input: `{"id":1, "name":"foo"}
{"id":3, "name":"bar"}
{"id":2, "name":"dummy"}`,
			expect: `{"id":1, "name":"foo"}
{"id":2, "name":"dummy"}
{"id":3, "name":"bar"}`,
			sort: Sort{
				Spec: Spec{Format: "JSON"},
				By: []Field{
					{
						Name:      "id",
						IsNumeric: true,
					},
				},
			},
		},

		{
			description: "multi field order by",

			input: `{"batch":"1", "id":1, "name":"foo"}
{"batch":"2", "id":1, "name":"bar"}
{"batch":"1", "id":3, "name":"bar"}
{"batch":"2", "id":20, "name":"star"}
{"batch":"2", "id":"2", "name":"dummy"}
{"batch":"1", "id":0, "name":"bar"}
`,
			expect: `{"batch":"1", "id":0, "name":"bar"}
{"batch":"1", "id":1, "name":"foo"}
{"batch":"1", "id":3, "name":"bar"}
{"batch":"2", "id":1, "name":"bar"}
{"batch":"2", "id":"2", "name":"dummy"}
{"batch":"2", "id":20, "name":"star"}`,
			sort: Sort{
				Spec: Spec{Format: "JSON"},
				By: []Field{
					{
						Name: "batch",
					},
					{
						Name:      "id",
						IsNumeric: true,
					},
				},
			},
		},
	}

	for _, useCase := range useCases[2:] {
		actual, _ := useCase.sort.Order(strings.NewReader(useCase.input), nil)
		output, _ := iou.ReadAll(actual)
		assert.EqualValues(t, useCase.expect, string(output), useCase.description)
	}
}
