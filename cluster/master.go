package cluster

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/conn"
	"github.com/lightjiang/webds/core"
	"github.com/lightjiang/webds/message"
	"time"
)

func NewMaster(selfID string, host string, port uint, path string, level int, c core.Cluster) core.Master {
	if c == nil {
		panic("cluster must be exist")
	}
	return &master{
		selfID:  selfID,
		level:   level,
		host:    host,
		port:    port,
		path:    path,
		url:     core.EncodeUrl(host, port, path),
		cluster: c,
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
	redirect      core.Master
	failedCount   uint
	// 存储向外的连接
	conn    core.Connection
	cluster core.Cluster
}

func (m *master) Level() int {
	return m.level
}

// 像目标节点请求连接， selfID 为本身节点id
func (m *master) Dial(cfg core.ConnCfg) {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%+v", e)
		}
	}()
	m.redirect = nil
	m.lastConnected = time.Now()
	if m.conn != nil && m.conn.Alive() {
		m.conn.Close()
	}
	var err error
	m.conn, err = conn.NewActiveConn(m.selfID, m.host, m.port, m.path, cfg)
	if err != nil {
		m.failedCount++
		return
	}
	m.conn.SetLevel(m.level)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Err(nil).Interface("panic", err).Msg("")
			}
		}()
		log.HandlerErrs(m.conn.Wait())
	}()
	m.failedCount = 0
	firstT := message.TopicClusterLateral
	if m.level > 0 {
		firstT = message.TopicClusterSuperior
	}
	stuck := make(chan bool, 1)
	m.conn.Subscribe(firstT, func(id string) {
		m.id = id
		if id == m.selfID {
			stuck <- false
			return
		}
		m.conn.SetTargetID(id)

		m.cluster.SendInfo(m.conn)
	})
	m.conn.Subscribe(message.TopicClusterInfo, func(s string) {
		m.cluster.UpdateFromInfo(m.conn, s)
		m.conn.Echo(message.TopicClusterRedirect, m.cluster.ID())
	}).SetOnce()
	m.conn.Subscribe(message.TopicClusterRedirect, func(url string) {
		if url != "" {
			temp := m.cluster.Search(url)
			if temp == nil {
				temp = m.cluster.AddUrl(url, m.Level())
			}
			m.redirect = temp
			stuck <- false
			return
		} else {
			m.conn.Subscribe(message.TopicClusterRedirect, func(url string) {
				if url != "" {
					temp := m.cluster.Search(url)
					if temp == nil {
						temp = m.cluster.AddUrl(url, m.Level())
					}
					m.redirect = temp
				}
			})
			m.redirect = nil
			stuck <- true
			for _, c := range m.cluster.Slave() {
				c.Echo(message.TopicClusterRedirect, m.Url())
				c.Close()
			}
		}
	}).SetOnce()
	m.conn.Echo(firstT, m.selfID)
	select {
	case b := <-stuck:
		if b {
			return
		}
	case <-time.After(time.Second * 3):
	}
	m.conn.Close()
	m.conn = nil
}

func (m *master) Redirect() core.Master {
	//if m.redirect != nil && m.redirect.NecessaryToConnect() {
	//	return m.redirect
	//}
	return m.redirect
}

func (m *master) Conn() core.Connection {
	return m.conn
}

func (m *master) SetID(id string) {
	m.id = id
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

func (m *master) NecessaryToConnect() bool {
	if m.Url() == "" {
		return false
	}
	if m.selfID == m.id {
		return false
	}
	if time.Now().Sub(m.lastConnected) < time.Second*5 {
		return false
	}
	if m.redirect != nil && time.Now().Sub(m.lastConnected) < time.Minute {
		return false
	}
	if m.failedCount > 5 && time.Now().Sub(m.lastConnected) < time.Minute {
		return false
	}
	return true
}

func (m *master) Alive() bool {
	return m.conn != nil && m.conn.Alive()
}
