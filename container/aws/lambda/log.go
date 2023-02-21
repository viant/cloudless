package lambda

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

func logStreamName(version string) string {
	randBuf := make([]byte, 16)
	rand.Read(randBuf)
	hexBuf := make([]byte, hex.EncodedLen(len(randBuf)))
	hex.Encode(hexBuf, randBuf)
	return time.Now().Format("2006/01/02") + "/[" + version + "]" + string(hexBuf)
}
