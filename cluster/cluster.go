package cluster

import (
	"fmt"
	"github.com/veypi/utils"
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
	cfg         *cfg.Config
	selfID      string
	level       uint
	master      *master
	masterChan  chan *master
	masters     []*master
	slaves      *sync.Map
	onConnected func(core.Connection)
	sync.Locker
}

func (c *cluster) String() string {
	return c.selfID
}

// 处理conn接收到的cluster topic 信息, 不论是主动还是被动连接
func (c *cluster) Receive(conn core.Connection, t message.Topic, data interface{}) {
	switch t.String() {
	case message.TopicClusterID.String():
		conn.SetClusterID(data.(string))
		if conn.Passive() {
			conn.Echo(message.TopicClusterID, c.ID())
			conn.Echo(message.TopicClusterLevel, int(c.level))
		}
	case message.TopicClusterLevel.String():
		if conn.Passive() {
			c.Lock()
			defer c.Unlock()
			c.addSlaveMaster(conn.ClusterID(), conn)
			if m := c.search(conn.ClusterID()); m != nil {
				m.level = uint(data.(int))
			}
			if c.master.Alive() {
				conn.Echo(message.TopicClusterRedirect, c.master.String())
			}
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
	if m.Conn() != nil && m.Alive() {
		m.Conn().Close()
	}
	m.failedCount++
	con, err := conn.NewActiveConn(c.ID(), m.host, m.port, m.path, c.cfg)
	if err != nil {
		return
	}
	m.SetConn(con)
	stuck := make(chan bool, 1)
	closed := false
	go func() {
		m.Conn().Wait()
		if !closed {
			stuck <- false
		}
	}()
	m.Conn().Subscribe(message.TopicClusterID, func(id string) {
		m.id = id
		m.Conn().SetTargetID(id)
	})
	m.Conn().Subscribe(message.TopicClusterRedirect, func(s string) {
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
			m.Conn().Close()
		} else if !closed {
			panic("it should not happened")
		}
	})
	m.Conn().Subscribe(message.TopicClusterLevel, func(data int) {
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
		if !closed {
			stuck <- res
		}
	})
	m.Conn().Echo(message.TopicClusterID, c.selfID)
	c.sendInfo(m.Conn())
	select {
	case res := <-stuck:
		closed = true
		close(stuck)
		if m.Alive() && res {
			m.failedCount = 0
			return
		}
	case <-time.After(time.Second * 3):
		log.Warn().Msg("waiting for connect to master timeout")
	}
	if m.Conn() != nil && m.Alive() {
		if m.Conn().ID() != c.selfID {
			panic("error")
		}
		m.Conn().Close()
	}
	m.SetConn(nil)
}

func (c *cluster) Start() {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%v", e)
		}
	}()
	ticker := time.NewTicker(time.Second * 1)
	c.tryMaster(c.nextTryToConnect())
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
			} else if m.Alive() && c.suitable(m) {
				log.Warn().Msgf("%s     %s: %p", c.cfg.Webds().String(), m.String(), m.Conn())
				m.Conn().Close()
				//c.setMaster(m)
			} else if c.suitable(m) {
				c.dial(m)
				if m.Alive() {
					if nm := c.nextTryToConnect(); m.level == c.level-1 && c.cfg.ClusterMode == 0 && c.suitable(nm) {
						m.Conn().Close()
						c.tryMaster(nm)
					} else {
						c.setMaster(m)
					}
				} else if m.redirect != nil && m.level == m.redirect.level {
					c.tryMaster(m.redirect)
				} else if nm := c.nextTryToConnect(); nm != nil {
					c.tryMaster(nm)
				}
			}
		case <-ticker.C:
			if count%100 == 0 && c.cfg.EnableAutoDetect && !c.master.Alive() {
				go c.autoSearchMaster()
			}
			count++
			if !c.master.Alive() {
				c.tryMaster(c.nextTryToConnect())
			}
		}
	}
}

func (c *cluster) tryMaster(m *master) {
	if !c.suitable(m) {
		return
	}
	c.masterChan <- m
}

func (c *cluster) setMaster(m *master) {
	c.Lock()
	defer c.Unlock()
	if !c.suitable(m) {
		return
	}
	c.master = m
	m.Conn().Echo(message.TopicClusterLevel, int(m.level))
	c.slaves.Range(func(key, value interface{}) bool {
		temp := value.(core.Connection)
		temp.Echo(message.TopicClusterRedirect, m.String())
		return true
	})
	id := m.Conn().ID()
	p := fmt.Sprintf("%p %p", m, m.Conn())
	m.Conn().OnDisconnect(func() {
		// 重试策略 先查是否有跳转，再重试，再全局寻找
		if m.Conn().ID() != id {
			log.Warn().Msgf("%p %p:%s %s closed %s %s", m, m.Conn(), c.selfID, m.Conn().String(), id, p)
			panic(p)
		}
		c.master = nil
		if m.redirect != nil && m.redirect.level == m.level {
			c.tryMaster(m.redirect)
			return
		}
		c.tryMaster(c.nextTryToConnect())
	}).SetOnce()
	if c.onConnected != nil {
		c.onConnected(m.Conn())
	}
	log.Info().Msgf("%s succeed to set {%s} as its master: %s", c.cfg.Webds().String(), m.String(), utils.CallPath(1))
}

// 添加平级从属节点
func (c *cluster) addSlaveMaster(id string, conn core.Connection) {
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
	if c.master.Alive() {
		return true
	}
	return c.nextTryToConnect() == nil
}

func (c *cluster) Master() core.Master {
	return c.master
}

func (c *cluster) nextTryToConnect() *master {
	var last *master
	for _, temp := range c.masters {
		if c.necessaryToConnect(temp) {
			if last == nil {
				last = temp
			} else {
				if temp.level == last.level+1 && c.cfg.ClusterMode == 0 {
					last = temp
				} else if temp.level == last.level && temp.lastConnected.Sub(last.lastConnected) < 0 {
					last = temp
				} else if temp.level == 0 {
					last = temp
				}
			}
		}
	}
	return last
}

func (c *cluster) suitable(m *master) bool {
	if m == nil {
		return false
	}
	if m.id == c.selfID {
		return false
	}
	if m.level == 0 || m.level == c.level-1 || (m.level == c.level && c.cfg.ClusterMode == 0) {
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
	if m.redirect != nil && m.redirect.id == c.selfID {
		return false
	}

	if sth, ok := c.slaves.Load(m.id); ok && sth != nil {
		return false
	}
	delta := time.Now().Sub(m.lastConnected)
	if delta < time.Second*5 {
		return false
	}
	if m.redirect != nil && delta < time.Minute {
		return false
	}
	// 指数间隔尝试
	if m.failedCount > 0 && delta < time.Second<<(m.failedCount+1) {
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

func (c *cluster) RangeCluster(fc func(conn core.Connection) bool) {
	c.slaves.Range(func(key, value interface{}) bool {
		return fc(value.(core.Connection))
	})
	if c.master.Alive() {
		fc(c.master.Conn())
	}
}

func (c *cluster) search(url string) *master {
	for _, m := range c.masters {
		if m.url == url || m.id == url {
			return m
		}
	}
	return nil
}

func (c *cluster) OnConnectedToMaster(fc func(connection core.Connection)) {
	c.onConnected = fc
}

func (c *cluster) autoSearchMaster() {
	defer func() {
		if e := recover(); e != nil {
			log.Warn().Msgf("%v", e)
		}
	}()
	ips := append(libs.GetLocalIps(), "127.0.0.1/32")
	log.Debug().Msgf("%s start auto search: %v", c.cfg.Webds().String(), ips)
	res := make([]string, 0, 10)
	for _, k := range ips {
		scanner, err := libs.NewScanner(k)
		if err != nil {
			continue
		}
		scanner.SetLimiter(10)
		// TODO 可能会阻塞住
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
