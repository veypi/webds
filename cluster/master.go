package cluster

import (
	"fmt"
	"github.com/lightjiang/webds/core"
	"time"
)

func newMaster(selfID string, host string, port uint, path string, level int) *master {
	return &master{
		selfID: selfID,
		level:  level,
		host:   host,
		port:   port,
		path:   path,
		url:    core.EncodeUrl(host, port, path),
	}
}

var _ core.Master = &master{}

type master struct {
	// 目标节点id
	id     string
	selfID string
	// 是否只能单向访问目标节点， 即目标节点是否为更高级节点
	level         int
	host          string
	port          uint
	path          string
	url           string
	latency       int
	lastConnected time.Time
	redirect      *master
	failedCount   uint
	// 存储向外的连接
	conn core.Connection
}

func (m *master) Level() int {
	return m.level
}

// 像目标节点请求连接， selfID 为本身节点id
func (m *master) Dial(cfg core.ConnCfg) {
}

func (m *master) Conn() core.Connection {
	return m.conn
}

func (m *master) String() string {
	return fmt.Sprintf("%s;%s;%d", m.Url(), m.id, m.level)
}

func (m *master) Url() string {
	if m.url == "" {
	}
	return m.url
}

func (m *master) ID() string {
	return m.id
}

func (m *master) necessaryToConnect() bool {
	if m.Url() == "" {
		return false
	}
	if m.selfID == m.id {
		return false
	}
	if m.redirect != nil && time.Now().Sub(m.lastConnected) < time.Minute {
		return false
	}
	if m.failedCount > 0 && time.Now().Sub(m.lastConnected) < time.Second<<(m.failedCount-1) {
		return false
	}
	return true
}

func (m *master) Alive() bool {
	return m.conn != nil && m.conn.Alive()
}
