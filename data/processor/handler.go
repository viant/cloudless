package processor

import (
	"context"
	"fmt"
)
//Handler represents custom processor handler, that allows creating a process per request URL
type Handler struct {
	handler func(ctx context.Context, request *Request) (Processor, error)
}


// Pre is an optional preprocessor interface
func (h *Handler) Pre(ctx context.Context, reporter Reporter) (context.Context, error) {
	handlerReporter, ok := reporter.(*HandlerReporter)
	if !ok {
		return nil, fmt.Errorf("expected: %T, but had: %T", handlerReporter, reporter)
	}
	var err error
	if handlerReporter.Processor, err = h.handler(ctx, &Request{SourceURL: reporter.BaseResponse().SourceURL}); err != nil {
		return nil, err
	}
	if preProcessor, ok := handlerReporter.Processor.(PreProcessor); ok {
		return preProcessor.Pre(ctx, reporter)
	}
	return ctx, nil
}


func (h *Handler) Process(ctx context.Context, data []byte, reporter Reporter) error {
	handlerReporter, ok := reporter.(*HandlerReporter)
	if !ok {
		return fmt.Errorf("expected: %T, but had: %T", handlerReporter, reporter)
	}
	return handlerReporter.Process(ctx, data, reporter)
}


//Post post processor
func (h *Handler) Post(ctx context.Context, reporter Reporter) error {
	handlerReporter, ok := reporter.(*HandlerReporter)
	if !ok {
		return fmt.Errorf("expected: %T, but had: %T", handlerReporter, reporter)
	}
	if poastProcessor, ok := handlerReporter.Processor.(PostProcessor); ok {
		return poastProcessor.Post(ctx, reporter)
	}
	return nil
}
//Handler create a custom handler processor (a dedicated process can be created based on processor.Request)
func NewHandler(handler func(ctx context.Context, request *Request) (Processor, error)) *Handler {
	return &Handler{handler: handler}
}

//HandlerReporter creates a handler reporter
type HandlerReporter struct {
	Reporter
	Processor
}

//NewHandlerReporter represents URL reporeter
func NewHandlerReporter(reporter Reporter) Reporter {
	return &HandlerReporter{Reporter: reporter}
}
