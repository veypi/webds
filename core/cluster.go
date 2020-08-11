package core

import "github.com/veypi/webds/message"

type Master interface {
	String() string
	Url() string
	ID() string
	// 返回节点级别
	Level() uint
	Alive() bool
	Conn() Connection
}

type Cluster interface {
	ID() string
	// 开始主动请求连接其他节点
	Start()
	// 被动接收到协议请求信息
	Receive(c Connection, t message.Topic, data interface{})
	Stable() bool
	Master() Master
	// 添加cluster节点地址
	Add(host string, port uint, path string) Master
	AddUrl(url string) Master
	Del(url string)
	RangeCluster(func(c Connection) bool)
	// 返回连接的下级cluster节点
	Slave() []Connection
	// 当cluster 连接到上级节点时触发
	OnConnectedToMaster(fc func(Connection))
}
