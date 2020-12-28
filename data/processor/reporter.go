package processor

import "time"

//Reporter represents interfcae providing processor response
type Reporter interface {
	//BaseResponse returns base response
	BaseResponse() *Response
}

type BaseReporter struct {
	*Response
	config *Config
}

//Response returns base response info
func (r *BaseReporter) BaseResponse() *Response {
	return r.Response
}

//NewReporter return reporter
func NewReporter() Reporter {
	result := &BaseReporter{
		Response: &Response{Status: StatusOk, statusSet: StatusSetOk, StartTime: time.Now()},
	}
	return result
}
