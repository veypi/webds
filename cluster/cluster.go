package cluster

import (
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/cfg"
	"github.com/veypi/webds/conn"
	"github.com/veypi/webds/core"
	"github.com/veypi/webds/libs"
	"github.com/veypi/webds/message"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var seed = rand.New(rand.NewSource(time.Now().Unix()))

func NewCluster(selfID string, cfg *cfg.Config) core.Cluster {
	return &cluster{
		cfg:        cfg,
		selfID:     selfID,
		level:      cfg.ClusterLevel,
		Locker:     &sync.Mutex{},
		masterChan: make(chan *master, 10),
		slaves:     &sync.Map{},
	}
}

var _ core.Cluster = &cluster{}

type cluster struct {
	cfg        *cfg.Config
	selfID     string
	level      uint
	master     *master
	masterChan chan *master
	masters    []*master
	slaves     *sync.Map
	sync.Locker
}

func (c *cluster) String() string {
	return c.selfID
}

// 处理conn接收到的cluster topic 信息, 不论是主动还是被动连接
func (c *cluster) Receive(conn core.Connection, t message.Topic, data interface{}) {
	switch t.String() {
	case message.TopicClusterID.String():
		if conn.Passive() {
			if c.master.Alive() {
				conn.Echo(message.TopicClusterRedirect, c.master.String())
			} else {
				c.addSlaveMaster(data.(string), conn)
			}
			conn.Echo(message.TopicClusterID, c.ID())
			conn.Echo(message.TopicClusterLevel, int(c.level))
		}
	case message.TopicClusterInfo.String():
		c.updateFromInfo(conn, data.(string))
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
	go m.conn.Wait()
	stuck := make(chan bool, 1)
	closed := false
	m.conn.Subscribe(message.TopicClusterID, func(id string) {
		m.id = id
		m.conn.SetTargetID(id)
	})
	m.conn.Subscribe(message.TopicClusterRedirect, func(s string) {
		data := strings.Split(s, ";")
		if len(data) != 3 {
			log.Warn().Msgf("receive invalid redirect data: %s", s)
			return
		}
		url, id, l := data[0], data[1], data[2]
		ll, err := strconv.Atoi(l)
		if err != nil {
			log.Warn().Msgf("receive invalid redirect data: %s", s)
		}
		level := uint(ll)
		temp := c.search(url)
		if temp == nil {
			temp = c.addUrl(url)
		}
		if id != "" && temp.id == "" {
			temp.id = id
		}
		if level > 0 && temp.level == 0 {
			temp.level = level
		}
		m.redirect = temp
		if temp.level == m.level && closed {
			m.conn.Close()
			c.masterChan <- temp
		}
	})
	m.conn.Subscribe(message.TopicClusterLevel, func(data int) {
		level := uint(data)
		m.level = level
		// 决定是否断开重新寻找目标
		res := false
		if c.suitable(m) {
			res = true
		}
		if sth, ok := c.slaves.Load(m.id); ok && sth != nil {
			res = false
		}
		stuck <- res
	})
	m.conn.Echo(message.TopicClusterID, m.selfID)
	c.sendInfo(m.conn)
	select {
	case res := <-stuck:
		closed = true
		close(stuck)
		if m.Alive() && res {
			return
		}
	case <-time.After(time.Second * 3):
		log.Warn().Msg("waiting for connect to master timeout")
	}
	if m.conn != nil && m.conn.Alive() {
		m.conn.Close()
	}
	m.conn = nil
}

func (c *cluster) Start() {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%v", e)
		}
	}()
	ticker := time.NewTicker(time.Second * 1)
	c.masterChan <- c.nextTryToConnect()
	count := 0
	for {
		select {
		// 为空时取消master, 不为空时: 有连接则置为master, 无连接则尝试连接
		case m := <-c.masterChan:
			// 仅在此处操作master
			if m == nil {
				// 置空
				if c.master.Alive() {
					c.master.Conn().Close()
				}
				c.master = nil
			} else if c.master.Alive() {
				//log.Warn().Msgf("ignore %s to replace %s", m.String(), c.master.String())
				// 略过
			} else if m.Alive() {
				if !c.suitable(m) {
					break
				}
				c.master = m
				c.slaves.Range(func(key, value interface{}) bool {
					temp := value.(core.Connection)
					temp.Echo(message.TopicClusterRedirect, m.String())
					return true
				})
				m.Conn().OnDisconnect(func() {
					// 重试策略 先查是否有跳转，再重试，再全局寻找
					c.master = nil
					c.masterChan <- c.nextTryToConnect()
				}).SetOnce()
				log.Info().Msgf("%s succeed to set {%s} as its master", c.cfg.Webds().String(), m.String())
			} else {
				log.Debug().Msgf("%s try to connect to %s", c.cfg.Webds().String(), m.String())
				c.dial(m)
				if m.Alive() {
					if c.suitable(m) {
						c.masterChan <- m
						break
					}
					m.conn.Close()
				}
				if m.redirect != nil && c.necessaryToConnect(m.redirect) {
					c.masterChan <- m.redirect
				} else if m = c.nextTryToConnect(); m != nil {
					c.masterChan <- m
				}
			}
		case <-ticker.C:
			if count%100 == 0 && c.cfg.EnableAutoDetect && !c.master.Alive() {
				c.autoSearchMaster()
			}
			count++
			if !c.Stable() {
				c.masterChan <- c.nextTryToConnect()
			}
		}
	}
}

// 添加平级从属节点
func (c *cluster) addSlaveMaster(id string, conn core.Connection) {
	c.slaves.Store(id, conn)
	conn.OnDisconnect(func() {
		c.slaves.Delete(id)
	}).SetOnce()
	for _, temp := range c.masters {
		if temp.id == id {
			temp.conn = conn
		}
	}
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
	if c.master.Alive() {
		return true
	}
	return c.nextTryToConnect() == nil
}

func (c *cluster) Master() core.Master {
	return c.master
}

func (c *cluster) nextTryToConnect() *master {
	for _, temp := range c.masters {
		if c.necessaryToConnect(temp) {
			return temp
		}
	}
	return nil
}

func (c *cluster) suitable(m *master) bool {
	if m.id == c.selfID {
		return false
	}
	if m.level == c.level-1 || (m.level == c.level && c.cfg.ClusterMode == 0) {
		return true
	}
	return false
}

func (c *cluster) necessaryToConnect(m *master) bool {
	if m == nil || m.Url() == "" || c.ID() == m.id || m.Alive() {
		return false
	}
	if m.level > 0 && !c.suitable(m) {
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

func (c *cluster) sendInfo(to core.Connection) {
	res := ""
	for _, temp := range c.masters {
		res += temp.String() + "\n"
	}
	to.Echo(message.TopicClusterInfo, res)
}

func (c *cluster) updateFromInfo(from core.Connection, info string) {
	for _, l := range strings.Split(info, "\n") {
		if p := strings.Split(l, ";"); len(p) == 3 {
			url, id, levelS := p[0], p[1], p[2]
			level, err := strconv.Atoi(levelS)
			if err != nil {
				log.Warn().Msgf("decode info failed: %s", l)
				continue
			}
			m := c.search(url)
			if m == nil {
				m = c.addUrl(url)
			}
			if m.id == "" {
				m.id = id
			}
			if m.level == 0 {
				m.level = uint(level)
			}
		}
	}
}

func (c *cluster) add(host string, port uint, path string) (res *master) {
	if (path == "" || path == "/") && c.cfg.ClusterSuffix != "" {
		path = c.cfg.ClusterSuffix
	}
	url := core.EncodeUrl(host, port, path)
	res = c.search(url)
	if res != nil {
		return res
	}
	c.Lock()
	defer c.Unlock()
	m := newMaster(c.ID(), host, port, path)
	c.masters = append(c.masters, m)
	res = m
	return res
}

func (c *cluster) Add(host string, port uint, path string) core.Master {
	return c.add(host, port, path)
}
func (c *cluster) addUrl(url string) *master {
	host, port, path := core.DecodeUrl(url)
	return c.add(host, port, path)
}
func (c *cluster) AddUrl(url string) core.Master {
	return c.addUrl(url)
}

func (c *cluster) Del(url string) {
	index := -1
	for i, m := range c.masters {
		if m.ID() == url || m.Url() == url {
			index = i
		}
	}
	if index >= 0 {
		c.Lock()
		defer c.Unlock()
		c.masters = append(c.masters[:index], c.masters[index+1:]...)
		return
	}
}

func (c *cluster) Range(f func(m core.Master) bool) {
	cdi := true
	for _, m := range c.masters {
		cdi = f(m)
		if !cdi {
			return
		}
	}
}

func (c *cluster) RangeConn(fc func(conn core.Connection) bool) {
	c.slaves.Range(func(key, value interface{}) bool {
		return fc(value.(core.Connection))
	})
}

func (c *cluster) search(url string) *master {
	for _, m := range c.masters {
		if m.url == url || m.id == url {
			return m
		}
	}
	return nil
}

func (c *cluster) autoSearchMaster() {
	ips := append(libs.GetLocalIps(), "127.0.0.1/32")
	log.Debug().Msgf("%s start auto search: %v", c.cfg.Webds().String(), ips)
	res := make([]string, 0, 10)
	for _, k := range ips {
		scanner, err := libs.NewScanner(k)
		if err != nil {
			log.Warn().Msg(err.Error())
			continue
		}
		r := scanner.ScanPortRange(c.cfg.ClusterPortMin, c.cfg.ClusterPortMax)
		//r := scanner.Scan()
		if len(r) > 0 {
			res = append(res, r...)
		}
	}
	for _, u := range res {
		c.addUrl(u)
	}
	log.Debug().Msgf("%s end auto search: %v", c.cfg.Webds().String(), res)
}
