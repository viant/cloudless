package processor

import (
	"context"
)

//Processor represents data processor
type Processor interface {
	Process(ctx context.Context, data []byte, reporter Reporter) error
}

// PreProcessor is an optional preprocessor interface
type PreProcessor interface {
	Pre(ctx context.Context, reporter Reporter) (context.Context, error)
}

// PostProcessor is an optional preprocessor interface
type PostProcessor interface {
	Post(ctx context.Context, reporter Reporter) error
}
