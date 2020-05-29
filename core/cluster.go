package core

import "github.com/veypi/webds/message"

type Master interface {
	String() string
	Url() string
	ID() string
	Level() int
	Alive() bool
	Conn() Connection
}

type Cluster interface {
	ID() string
	// 开始主动请求连接其他节点
	Start()
	// 被动接收到协议请求信息
	Receive(c Connection, t message.Topic, data string)
	Stable() bool
	Master() Master
	Add(host string, port uint, path string, level int) Master
	AddUrl(url string, level int) Master
	Del(url string)
	Range(func(m Master) bool)
	RangeConn(func(c Connection) bool)
	Slave() []Connection
}
