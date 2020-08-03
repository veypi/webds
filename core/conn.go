package core

import (
	"context"
	"github.com/veypi/webds/message"
	"io"
	"net/http"
)

type (
	ErrorFunc      = message.FuncError
	DisconnectFunc = message.FuncBlank
	ConnectFunc    = message.FuncBlank
)

type Connection interface {
	ID() string
	String() string
	Request() *http.Request

	Set(string, interface{})
	Get(string) interface{}
	Ctx() context.Context

	// 判断是个被动建立的连接还是主动建立的连接
	Passive() bool
	ClusterID() string
	SetClusterID(string)
	TargetID() string
	TargetUrl() string
	SetTargetID(string)
	SetTargetHost(string)
	SetTargetPort(uint)
	SetTargetPath(string)

	// 该方法仅主动方有效
	OnConnect(ConnectFunc) *message.Subscriber
	OnDisconnect(DisconnectFunc) *message.Subscriber
	OnError(ErrorFunc) *message.Subscriber
	FireOnError(err error)
	// 发送给对方节点广播消息
	Publisher(string) func(interface{})
	// 发送给对方节点非广播消息
	Echo(topic message.Topic, data interface{})
	// 通知对方自己订阅消息，并注册响应函数
	Subscribe(message.Topic, message.Func) *message.Subscriber
	Wait() error
	io.WriteCloser
	Type() int
	Alive() bool
}
