package cluster

import (
	"fmt"
	"github.com/veypi/webds/core"
	"time"
)

func newMaster(selfID string, host string, port uint, path string) *master {
	return &master{
		selfID: selfID,
		host:   host,
		port:   port,
		path:   path,
		url:    core.EncodeUrl(host, port, path),
	}
}

var _ core.Master = &master{}

type master struct {
	// 目标节点id
	id            string
	selfID        string
	level         uint
	host          string
	port          uint
	path          string
	url           string
	latency       int
	lastConnected time.Time
	redirect      *master
	failedCount   uint
	// 存储连接
	_conn core.Connection
}

func (m *master) Level() uint {
	return m.level
}

func (m *master) Conn() core.Connection {
	return m._conn
}

func (m *master) SetConn(c core.Connection) {
	m._conn = c
}

func (m *master) String() string {
	return fmt.Sprintf("%s;%s;%d", m.Url(), m.id, m.level)
}

func (m *master) Url() string {
	if m.url == "" {
		m.url = core.EncodeUrl(m.host, m.port, m.path)
	}
	return m.url
}

func (m *master) ID() string {
	return m.id
}

func (m *master) Alive() bool {
	return m != nil && m._conn != nil && m._conn.Alive()
}
