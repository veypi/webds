package cluster

import (
	"fmt"
	"github.com/lightjiang/utils/log"
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
		masterChan: make(chan core.Master, 10),
	}
}

var _ core.Cluster = &cluster{}

type cluster struct {
	cfg             core.ConnCfg
	selfID          string
	master          core.Master
	masterChan      chan core.Master
	superiorMasters []core.Master
	lateralMasters  []core.Master
	slaves          sync.Map
	sync.Locker
}

func (c *cluster) Receive(conn core.Connection, t message.Topic, data string) {
	switch t.String() {
	case message.TopicClusterLateral.String():
		conn.Echo(message.TopicClusterLateral, c.ID())
		conn.SetLevel(0)
		if data == c.ID() {
			conn.Close()
		}
		if c.master == nil || !c.master.Alive() && c.master.Level() != 0 {
			c.AddSlaveMaster(data, conn)
		}
	case message.TopicClusterSuperior.String():
		conn.Echo(message.TopicClusterSuperior, c.ID())
		conn.SetLevel(-1)
		if data == c.ID() {
			conn.Close()
		}
	case message.TopicClusterInfo.String():
		c.UpdateFromInfo(conn, data)
		c.SendInfo(conn)
	case message.TopicClusterRedirect.String():
		if c.master != nil && c.master.Alive() &&
			conn.Level() == c.master.Level() {
			conn.Echo(message.TopicClusterRedirect, c.master.Url())
		} else if data == c.ID() {
			conn.Echo(message.TopicClusterRedirect, c.ID())
			conn.Close()
		} else {
			conn.Echo(message.TopicClusterRedirect, "")
		}
	}
}

func (c *cluster) Start() {
	go c.start()
	time.Sleep(time.Duration(seed.Int63n(1000)) * time.Millisecond)
	c.masterChan <- c.NextTryToConnect()
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
					if m.Redirect() != nil {
						c.masterChan <- m.Redirect()
					} else if m.NecessaryToConnect() {
						c.masterChan <- m
					} else {
						c.masterChan <- c.NextTryToConnect()
					}
				}).SetOnce()
				log.Info().Msgf("%s succeed to set {%s} as its master", c.ID(), m.String())
			} else {
				m.Dial(c.cfg)
				log.Debug().Msgf("%s try to connect to %s %v", c.ID(), m.String(), m.Alive())
				if m.Alive() {
					c.masterChan <- m
				} else if m.Redirect() != nil {
					c.masterChan <- m.Redirect()
				} else if m = c.NextTryToConnect(); m != nil {
					c.masterChan <- m
				}
			}
		case <-ticker.C:
			log.Warn().Msgf("%s check %v", c.ID(), c.Stable())
			if !c.Stable() {
				c.masterChan <- c.NextTryToConnect()
			}
		}
	}
}

func (c *cluster) AddSlaveMaster(id string, conn core.Connection) {
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
	return c.NextTryToConnect() == nil
}

func (c *cluster) Master() core.Master {
	return c.master
}

func (c *cluster) NextTryToConnect() core.Master {
	for _, temp := range c.lateralMasters {
		if temp.NecessaryToConnect() {
			return temp
		}
	}
	for _, temp := range c.superiorMasters {
		if temp.NecessaryToConnect() {
			return temp
		}
	}
	return nil
}

func (c *cluster) SendInfo(to core.Connection) {
	res := ""
	for _, temp := range c.lateralMasters {
		res += fmt.Sprintf("%s,%s,0\n", temp.Url(), temp.ID())
	}
	for _, temp := range c.superiorMasters {
		res += fmt.Sprintf("%s,%s,1\n", temp.Url(), temp.ID())
	}
	to.Echo(message.TopicClusterInfo, res)
}

func (c *cluster) UpdateFromInfo(from core.Connection, info string) {
	for _, l := range strings.Split(info, "\n") {
		if p := strings.Split(l, ","); len(p) == 3 {
			url, id, levelS := p[0], p[1], p[2]
			founded := false
			level, err := strconv.Atoi(levelS)
			if err != nil {
				log.Warn().Msgf("decode info failed: %s", l)
				continue
			}
			c.Range(func(m core.Master) bool {
				if m.ID() == id || m.Url() == url {
					founded = true
					if m.ID() == "" {
						m.SetID(id)
					}
					return false
				}
				return true
			})
			if !founded {
				if from.Level() > 0 && level == 0 {
					c.AddUrl(url, 1).SetID(id)
				} else if from.Level() == 0 {
					c.AddUrl(url, level).SetID(id)
				} else if level > 0 {
					c.AddUrl(url, 0).SetID(id)
				}
			}
		}
	}
}

func (c *cluster) Add(host string, port uint, path string, level int) (res core.Master) {
	url := core.EncodeUrl(host, port, path)
	c.Range(func(m core.Master) bool {
		if url == m.Url() {
			res = m
			return false
		}
		return true
	})
	if res == nil {
		c.Lock()
		defer c.Unlock()
		m := NewMaster(c.ID(), host, port, path, level, c)
		if level > 0 {
			c.superiorMasters = append(c.superiorMasters, m)
		} else if level == 0 {
			c.lateralMasters = append(c.lateralMasters, m)
		}
		res = m
	}
	return res
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

func (c *cluster) Search(url string) core.Master {
	var res core.Master
	c.Range(func(m core.Master) bool {
		if m.ID() == url || m.Url() == url {
			res = m
			return false
		}
		return true
	})
	return res
}
