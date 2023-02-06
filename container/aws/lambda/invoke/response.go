package invoke

import "github.com/viant/cloudless/container/aws/lambda/shared"

type Response struct {
	Payload []byte
	Error   *shared.Error
}
