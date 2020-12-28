package processor

import (
	"bufio"
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/cloudless/ioutil"
	"io"
	"sort"
	"strconv"
	"strings"
)

type (
	Spec struct {
		Format    string
		Delimiter string
	}

	//Sort represents configuration sort definition
	Sort struct {
		Spec
		By    []Field
		Batch bool //batches data by first sorted field
	}
	//Sort represents sort field definition
	Field struct {
		Name      string
		Index     int
		IsNumeric bool
	}

	//Sortables represent sortable items
	Sortables struct {
		Sort
		Items [][]byte
	}
)

//Fields
type Fields struct {
	Sort
	values map[string]interface{}
}

// Implementing Unmarshaler
func (f *Fields) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	var field *Field
	for i, item := range f.By {
		if item.Name == k {
			field = &f.By[i]
			break
		}
	}
	if field == nil {
		return nil
	}
	embedded := gojay.EmbeddedJSON{}
	err := dec.EmbeddedJSON(&embedded)
	if err != nil {
		return err
	}
	text := string(embedded)
	text = strings.Trim(text, `"`)
	if field.IsNumeric {
		intVal, _ := strconv.Atoi(text)
		f.values[k] = intVal
		return nil
	}
	f.values[k] = text
	return nil
}

// we return 0, it tells the Decoder to decode all keys
func (f Fields) NKeys() int {
	return len(f.Sort.By)
}

func (f *Field) Value(data []byte, spec *Spec) interface{} {
	if strings.ToLower(spec.Format) == "csv" {
		record := bytes.Split(data, []byte(spec.Delimiter))
		if f.Index >= len(record) {
			return ""
		}
		return record[f.Index]
	}
	fields := Fields{values: map[string]interface{}{}, Sort: Sort{By: []Field{*f}}}
	gojay.Unmarshal(data, &fields)
	return fields.values[f.Name]
}

//Order orders the reader data
func (s Sort) Order(reader io.Reader, config *Config) (io.Reader, error) {
	scanner := bufio.NewScanner(reader)
	if config != nil {
		config.AdjustScannerBuffer(scanner)
	}
	var sorables = &Sortables{
		Sort:  s,
		Items: make([][]byte, 0),
	}
	count := 0
	for scanner.Scan() {
		bs := scanner.Bytes()
		if len(bs) == 0 {
			continue
		}
		count++
		item := make([]byte, len(bs)+1)
		copy(item, bs)
		item[len(item)-1] = '\n'
		sorables.Items = append(sorables.Items, item)
	}
	err := scanner.Err()
	if err == io.EOF {
		err = nil
	}
	sort.Sort(sorables)
	////remove new line from the last item
	lastIndex := len(sorables.Items) - 1
	if lastIndex >= 0 {
		last := sorables.Items[lastIndex]
		sorables.Items[lastIndex] = bytes.Trim(last, "\n")
	}
	return ioutil.BytesSliceReader(sorables.Items), nil
	//data := bytes.Join(sorables.Items, []byte("\n"))
	//return bytes.NewReader(data), err
}

// Len is part of sort.Interface.
func (s *Sortables) Len() int {
	return len(s.Items)
}

// Swap is part of sort.Interface.
func (s *Sortables) Swap(i, j int) {
	s.Items[i], s.Items[j] = s.Items[j], s.Items[i]
}

// Less is part of sort.Interface
func (s *Sortables) Less(srcIdx, destIdx int) bool {
	switch strings.ToLower(s.Format) {
	case "csv":
		return s.csvLess(srcIdx, destIdx)
	}
	return s.jsonLess(srcIdx, destIdx)
}

func (s *Sortables) csvLess(srcIdx int, destIdx int) bool {
	delimiter := s.Delimiter
	if delimiter == "" {
		delimiter = ","
	}
	src := bytes.Split(s.Items[srcIdx], []byte(delimiter))
	dest := bytes.Split(s.Items[destIdx], []byte(delimiter))
	for _, field := range s.By {
		if field.IsNumeric {
			srcValue := bytesToInt(src, field.Index)
			destValue := bytesToInt(dest, field.Index)
			if srcValue != destValue {
				return srcValue < destValue
			}
			continue
		}
		srcValue := bytesToString(src, field.Index)
		destValue := bytesToString(dest, field.Index)
		if srcValue != destValue {
			return srcValue < destValue
		}
	}
	return false
}

func (s *Sortables) jsonLess(srcIdx int, destIdx int) bool {
	src := Fields{values: map[string]interface{}{}, Sort: s.Sort}
	dest := Fields{values: map[string]interface{}{}, Sort: s.Sort}
	gojay.Unmarshal(s.Items[srcIdx], &src)
	gojay.Unmarshal(s.Items[destIdx], &dest)
	if len(src.values) == 0 {
		return true
	}
	if len(dest.values) == 0 {
		return false
	}

	for _, field := range s.By {
		if field.IsNumeric {
			srcValue := entryToFloat(src.values, field.Name)
			destValue := entryToFloat(dest.values, field.Name)
			if srcValue != destValue {
				return srcValue < destValue
			}
			continue
		}
		srcValue := entryToString(src.values, field.Name)
		destValue := entryToString(dest.values, field.Name)

		if srcValue != destValue {
			return srcValue < destValue
		}
	}
	return true
}

func entryToFloat(aMap map[string]interface{}, key string) float64 {
	val, ok := aMap[key]
	if !ok {
		return 0
	}
	switch actual := val.(type) {
	case int64:
		return float64(actual)
	case uint64:
		return float64(actual)
	case int:
		return float64(actual)
	case string:
		v, _ := strconv.Atoi(actual)
		return float64(v)
	case float64:
		return actual
	}
	return 0
}

func entryToString(aMap map[string]interface{}, key string) string {
	val, ok := aMap[key]
	if !ok {
		return ""
	}
	switch actual := val.(type) {
	case int64:
		return strconv.Itoa(int(actual))
	case uint64:
		return strconv.Itoa(int(actual))
	case int:
		return strconv.Itoa(actual)
	case string:
		return actual

	}
	return ""
}

func bytesToString(data [][]byte, index int) string {
	if index >= len(data) {
		return ""
	}
	return string(data[index])
}

func bytesToInt(data [][]byte, index int) int {
	if index >= len(data) {
		return 0
	}
	val, _ := strconv.Atoi(string(data[index]))
	return val
}
