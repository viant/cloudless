package main

import (
	"github.com/viant/cloudless/container/aws/sam"
	"os"
)

func main() {
	sam.Run(os.Args[1:])
}
