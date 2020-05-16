package cluster

import (
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
	// 存储连接
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
	if m.level > 0 {
		return "(s)" + m.Url() + ";" + m.id
	} else if m.level == 0 {
		return "(l)" + m.Url() + ";" + m.id
	}
	return m.Url() + ";" + m.id
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
	if m == nil || m.Url() == "" || m.selfID == m.id || m.Alive() {
		return false
	}
	delta := time.Now().Sub(m.lastConnected)
	if delta < time.Millisecond*100 {
		return false
	}
	if m.redirect != nil && delta < time.Minute {
		return false
	}
	// 指数间隔尝试
	if m.failedCount > 0 && delta < time.Second<<(m.failedCount-1) {
		return false
	}
	return true
}

func (m *master) Alive() bool {
	return m != nil && m.conn != nil && m.conn.Alive()
}
