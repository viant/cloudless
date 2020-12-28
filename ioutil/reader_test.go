package ioutil

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

func TestBytesSliceReader(t *testing.T) {

	var useCases = []struct {
		description string
		input       [][]byte
		expect      string
	}{

		{
			description: "small buffer",
			input: [][]byte{
				[]byte(strings.Repeat("1", 10) + "\n"),
				[]byte(strings.Repeat("2", 10) + "\n"),
				[]byte(strings.Repeat("3", 10) + ""),
			},
			expect: strings.Repeat("1", 10) + "\n" +
				strings.Repeat("2", 10) + "\n" +
				strings.Repeat("3", 10),
		},
		{
			description: "medium buffer",
			input: [][]byte{
				[]byte(strings.Repeat("1", 512) + "\n"),
				[]byte(strings.Repeat("2", 512) + "\n"),
				[]byte(strings.Repeat("3", 512) + ""),
			},
			expect: strings.Repeat("1", 512) + "\n" +
				strings.Repeat("2", 512) + "\n" +
				strings.Repeat("3", 512),
		},
		{
			description: "large buffer",
			input: [][]byte{
				[]byte(strings.Repeat("1", 712) + "\n"),
				[]byte(strings.Repeat("2", 712) + "\n"),
				[]byte(strings.Repeat("3", 712) + ""),
			},
			expect: strings.Repeat("1", 712) + "\n" +
				strings.Repeat("2", 712) + "\n" +
				strings.Repeat("3", 712),
		},
		{
			description: "extra large buffer",
			input: [][]byte{
				[]byte(strings.Repeat("1", 2012) + "\n"),
				[]byte(strings.Repeat("2", 217) + "\n"),
				[]byte(strings.Repeat("3", 712) + ""),
			},
			expect: strings.Repeat("1", 2012) + "\n" +
				strings.Repeat("2", 217) + "\n" +
				strings.Repeat("3", 712),
		},
		{
			description: "extra large buffer",
			input: [][]byte{
				[]byte(strings.Repeat("1", 201200) + "\n"),
				[]byte(strings.Repeat("2", 21700) + "\n"),
				[]byte(strings.Repeat("3", 71200) + ""),
			},
			expect: strings.Repeat("1", 201200) + "\n" +
				strings.Repeat("2", 21700) + "\n" +
				strings.Repeat("3", 71200),
		},
	}

	for _, useCase := range useCases {

		reader := BytesSliceReader(useCase.input)
		//512
		actual, err := ioutil.ReadAll(reader)
		assert.Nil(t, err, useCase.description)
		assert.EqualValues(t, useCase.expect, string(actual))

	}

}
