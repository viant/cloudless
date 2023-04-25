package lambda

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"time"
)

func logStreamName(version string) string {
	randBuf := make([]byte, 16)
	rand.Read(randBuf)
	hexBuf := make([]byte, hex.EncodedLen(len(randBuf)))
	hex.Encode(hexBuf, randBuf)
	return time.Now().Format("2006/01/02") + "/[" + version + "]" + string(hexBuf)
}

func Log(stream io.WriteCloser, msg string) {
	os.Stdout.WriteString(msg)
	stream.Write([]byte(msg))
}

func logStartRequest(ctx *Context, stream io.WriteCloser) {
	Log(stream, ctx.Message("START RequestId: "+ctx.RequestID))
}

func logInvokeStart(ctx *Context, stream io.WriteCloser) {
	Log(stream, ctx.Message("INVOKE Request: \n\t "+string(ctx.InvokePayload)))
}

func logInvokeEnd(ctx *Context, stream io.WriteCloser) {
	Log(stream, ctx.Message("INVOKE Response: \n\t"+string(ctx.InvokeResponse)))
}

func logInvokeError(ctx *Context, stream io.WriteCloser) {
	Log(stream, ctx.Message("Invoke ERROR: "+ctx.RequestID+"\n\terror: "+ctx.InvokeError))
}

func logEndRequest(ctx *Context, stream io.WriteCloser) {
	Log(stream, ctx.Message("END RequestId: "+ctx.RequestID+" Elapsed: "+time.Now().Sub(ctx.Start).String()))
}
