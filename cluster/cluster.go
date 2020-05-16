package cluster

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/conn"
	"github.com/lightjiang/webds/core"
	"github.com/lightjiang/webds/message"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var seed = rand.New(rand.NewSource(time.Now().Unix()))

func NewCluster(selfID string, cfg core.ConnCfg) core.Cluster {
	return &cluster{
		cfg:        cfg,
		selfID:     selfID,
		Locker:     &sync.Mutex{},
		masterChan: make(chan *master, 10),
	}
}

var _ core.Cluster = &cluster{}

type cluster struct {
	cfg             core.ConnCfg
	selfID          string
	master          *master
	masterChan      chan *master
	superiorMasters []*master
	lateralMasters  []*master
	slaves          sync.Map
	sync.Locker
}

// 处理conn接收到的cluster topic 信息, 不论是主动还是被动连接
func (c *cluster) Receive(conn core.Connection, t message.Topic, data string) {
	switch t.String() {
	case message.TopicClusterLateral.String():
		conn.SetLevel(0)
		if conn.Passive() {
			if c.master != nil && c.master.Alive() && c.master.Level() == 0 {
				conn.Echo(message.TopicClusterRedirect, c.master.Url())
			} else {
				c.AddSlaveMaster(data, conn)
			}
			conn.Echo(message.TopicClusterLateral, c.ID())
		}
		if data == c.ID() {
			conn.Close()
		}
	case message.TopicClusterSuperior.String():
		if data == c.ID() {
			conn.Close()
		}
		if conn.Passive() {
			conn.SetLevel(-1)
			conn.Echo(message.TopicClusterSuperior, c.ID())
		} else {
			conn.SetLevel(1)
		}
	case message.TopicClusterInfo.String():
		c.updateFromInfo(conn, data)
		if conn.Passive() {
			c.sendInfo(conn)
		}
	}
}

func (c *cluster) dial(m *master) {
	m.redirect = nil
	m.lastConnected = time.Now()
	if m.conn != nil && m.conn.Alive() {
		m.conn.Close()
	}
	var err error
	m.conn, err = conn.NewActiveConn(c.ID(), m.host, m.port, m.path, c.cfg)
	if err != nil {
		m.failedCount++
		return
	}
	m.failedCount = 0
	m.conn.SetLevel(m.level)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Err(nil).Interface("panic", err).Msg("")
			}
		}()
		log.HandlerErrs(m.conn.Wait())
	}()
	firstT := message.TopicClusterLateral
	m.conn.SetLevel(0)
	if m.level > 0 {
		firstT = message.TopicClusterSuperior
		m.conn.SetLevel(1)
	}
	stuck := make(chan bool, 1)
	m.conn.Subscribe(firstT, func(id string) {
		m.id = id
		m.conn.SetTargetID(id)
		stuck <- true
	})
	m.conn.Subscribe(message.TopicClusterRedirect, func(url string) {
		if url != "" {
			temp := c.search(url)
			if temp == nil {
				temp = c.addUrl(url, m.Level())
			}
			m.redirect = temp
		}
	})
	m.conn.Echo(firstT, m.selfID)
	c.sendInfo(m.conn)
	select {
	case <-stuck:
		if m.Alive() {
			if m.redirect != nil {
				m.conn.Close()
				m.conn = nil
			}
			return
		}
	case <-time.After(time.Second * 3):
		m.conn.Close()
	}
	m.conn = nil
}

func (c *cluster) Start() {
	go c.start()
	time.Sleep(time.Duration(seed.Int63n(1000)) * time.Millisecond)
	c.masterChan <- c.nextTryToConnect()
}

func (c *cluster) start() {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%v", e)
		}
	}()
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		// 为空时取消master, 不为空时: 有连接则置为master, 无连接则尝试连接
		case m := <-c.masterChan:
			// 仅在此处操作master
			if m == nil {
				// 置空
				if c.master != nil && c.master.Alive() {
					c.master.Conn().Close()
				}
				c.master = nil
			} else if m.Alive() {
				// 仅在该处写入s.master
				if c.master != nil {
					if c.master.Url() == m.Url() {
						// 发送重复
						log.Warn().Msg("333")
						break
					}
					if c.master.Alive() {
						c.master.Conn().Close()
					}
				}
				c.master = m
				m.Conn().OnDisconnect(func() {
					// 重试策略 先查是否有跳转，再重试，再全局寻找
					c.master = nil
					if m.redirect != nil {
						c.masterChan <- m.redirect
					} else if m.necessaryToConnect() {
						c.masterChan <- m
					} else {
						c.masterChan <- c.nextTryToConnect()
					}
				}).SetOnce()
				log.Info().Msgf("%s succeed to set {%s} as its master", c.ID(), m.String())
			} else {
				c.dial(m)
				log.Debug().Msgf("%s try to connect to %s %v", c.ID(), m.String(), m.Alive())
				if m.Alive() {
					c.masterChan <- m
				} else if m.redirect != nil {
					c.masterChan <- m.redirect
				} else if m = c.nextTryToConnect(); m != nil {
					c.masterChan <- m
				}
			}
		case <-ticker.C:
			log.Warn().Msgf("%s check %v", c.ID(), c.Stable())
			if !c.Stable() {
				c.masterChan <- c.nextTryToConnect()
			}
		}
	}
}

// 添加平级从属节点
func (c *cluster) AddSlaveMaster(id string, conn core.Connection) {
	if conn.Level() != 0 {
		return
	}
	c.slaves.Store(id, conn)
	conn.OnDisconnect(func() {
		c.slaves.Delete(id)
	}).SetOnce()
}

func (c *cluster) Slave() []core.Connection {
	res := make([]core.Connection, 0, 10)
	c.slaves.Range(func(key, value interface{}) bool {
		res = append(res, value.(core.Connection))
		return true
	})
	return res
}

func (c *cluster) ID() string {
	return c.selfID
}

func (c *cluster) Stable() bool {
	if c.master != nil && c.master.Alive() {
		return true
	}
	return c.nextTryToConnect() == nil
}

func (c *cluster) Master() core.Master {
	return c.master
}

func (c *cluster) nextTryToConnect() *master {
	for _, temp := range c.lateralMasters {
		if temp.necessaryToConnect() {
			return temp
		}
	}
	for _, temp := range c.superiorMasters {
		if temp.necessaryToConnect() {
			return temp
		}
	}
	return nil
}

func (c *cluster) sendInfo(to core.Connection) {
	res := ""
	for _, temp := range c.lateralMasters {
		res += fmt.Sprintf("%s,%s,0\n", temp.Url(), temp.ID())
	}
	for _, temp := range c.superiorMasters {
		res += fmt.Sprintf("%s,%s,1\n", temp.Url(), temp.ID())
	}
	to.Echo(message.TopicClusterInfo, res)
}

func (c *cluster) updateFromInfo(from core.Connection, info string) {
	for _, l := range strings.Split(info, "\n") {
		if p := strings.Split(l, ","); len(p) == 3 {
			url, id, levelS := p[0], p[1], p[2]
			founded := false
			level, err := strconv.Atoi(levelS)
			if err != nil {
				log.Warn().Msgf("decode info failed: %s", l)
				continue
			}
			if m := c.search(url); m != nil {
				founded = true
				if m.id == "" {
					m.id = id
				}
			}
			if !founded {
				if from.Level() > 0 && level == 0 {
					c.addUrl(url, 1).id = id
				} else if from.Level() == 0 {
					c.addUrl(url, level).id = id
				} else if level > 0 {
					c.addUrl(url, 0).id = id
				}
			}
		}
	}
}

func (c *cluster) add(host string, port uint, path string, level int) (res *master) {
	url := core.EncodeUrl(host, port, path)
	res = c.search(url)
	if res != nil {
		return res
	}
	c.Lock()
	defer c.Unlock()
	m := newMaster(c.ID(), host, port, path, level)
	if level > 0 {
		c.superiorMasters = append(c.superiorMasters, m)
	} else if level == 0 {
		c.lateralMasters = append(c.lateralMasters, m)
	}
	res = m
	return res
}

func (c *cluster) Add(host string, port uint, path string, level int) core.Master {
	return c.add(host, port, path, level)
}
func (c *cluster) addUrl(url string, level int) *master {
	host, port, path := core.DecodeUrl(url)
	return c.add(host, port, path, level)
}
func (c *cluster) AddUrl(url string, level int) core.Master {
	host, port, path := core.DecodeUrl(url)
	return c.Add(host, port, path, level)
}

func (c *cluster) Del(url string) {
	index := -1
	for i, m := range c.lateralMasters {
		if m.ID() == url || m.Url() == url {
			index = i
		}
	}
	if index >= 0 {
		c.Lock()
		defer c.Unlock()
		c.lateralMasters = append(c.lateralMasters[:index], c.lateralMasters[index+1:]...)
		return
	}
	for i, m := range c.superiorMasters {
		if m.ID() == url || m.Url() == url {
			index = i
		}
	}
	if index >= 0 {
		c.Lock()
		defer c.Unlock()
		c.superiorMasters = append(c.superiorMasters[:index], c.superiorMasters[index+1:]...)
		return
	}
}

func (c *cluster) Range(f func(m core.Master) bool) {
	cdi := true
	for _, m := range c.lateralMasters {
		cdi = f(m)
		if !cdi {
			return
		}
	}
	for _, m := range c.superiorMasters {
		cdi = f(m)
		if !cdi {
			return
		}
	}
}

func (c *cluster) search(url string) *master {
	for _, m := range c.lateralMasters {
		if m.url == url || m.id == url {
			return m
		}
	}
	for _, m := range c.superiorMasters {
		if m.url == url || m.id == url {
			return m
		}
	}
	return nil
}
