package lambda

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"io"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path"
	"strconv"
	"syscall"
	"time"
)

const connectionMaxAttempt = 100

type Function struct {
	Config *FunctionConfig
	Cmd    *exec.Cmd
	Port   int
	Out    bytes.Buffer
	client *rpc.Client
}

func (f *Function) Start(ctx context.Context, port int) error {
	f.Port = port
	var cmd *exec.Cmd
	if debug := f.Config.Debug; debug != nil && debug.Enabled {
		delveArgs := []string{
			"--listen=:" + strconv.Itoa(debug.Delve.Port),
			"--headless=true",
			"--api-version=" + debug.Delve.API,
			"--log",
			"exec",
			"/var/task/" + *f.Config.Handler,
		}
		cmd = exec.Command(debug.Delve.Location, delveArgs...)
	} else {
		location := path.Join(f.Config.FuncLocation, *f.Config.FunctionName, *f.Config.Handler)
		cmd = exec.Command(location)
	}
	cmd.Stdout = io.MultiWriter(os.Stdout, &f.Out)
	cmd.Stderr = io.MultiWriter(os.Stderr, &f.Out)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := f.Config.AddEnv(ctx, &cmd.Env, port, ""); err != nil {
		return err
	}
	err := cmd.Start()
	if err == nil {
		f.Cmd = cmd
	}
	return err
}

func (f *Function) Stop() error {
	if f.Cmd == nil || f.Cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-f.Cmd.Process.Pid, syscall.SIGKILL)
}

func (f *Function) Client() (*rpc.Client, error) {
	if f.client != nil {
		return f.client, nil
	}
	conn, err := f.clientConnection()
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	for i := 0; i < connectionMaxAttempt; i++ {
		err = client.Call("Function.Ping", messages.PingRequest{}, &messages.PingResponse{})
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (f *Function) Call(ctx context.Context, request *messages.InvokeRequest) (*messages.InvokeResponse, error) {
	client, err := f.Client()
	if err != nil {
		return nil, err
	}
	var response *messages.InvokeResponse
	err = client.Call("Function.Invoke", request, &response)
	return response, err
}

func (f *Function) clientConnection() (net.Conn, error) {
	for i := 0; i < connectionMaxAttempt; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%v", f.Port))
		if err == nil {
			return conn, err
		}
		if oerr, ok := err.(*net.OpError); ok { // Connection refused, try again
			if oerr.Op == "dial" && oerr.Net == "tcp" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
	}
	return nil, fmt.Errorf("failed to connect: %v", f.Port)
}

//NewFunction creates a function
func NewFunction(config *FunctionConfig) (*Function, error) {
	ret := &Function{Config: config}
	return ret, nil
}
