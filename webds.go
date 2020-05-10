package webds

import (
	"context"
	"errors"
	"fmt"
	"github.com/lightjiang/utils/log"
	client "github.com/lightjiang/webds/client/go.client"
	"github.com/lightjiang/webds/message"
	"github.com/lightjiang/webds/trie"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	Version = "v0.2.4"
)

var seed = rand.New(rand.NewSource(time.Now().Unix()))

type ConnectionFunc func(Connection) error

type serverMaster struct {
	isSuperior    bool
	url           string
	id            string
	latency       int
	lastConnected time.Time
	redirect      *serverMaster
	failedCount   uint
	// 存储向外的连接
	conn *specialConn
}

func (s *serverMaster) String() string {
	return fmt.Sprintf("%s;%s;%v", s.url, s.id, s.isSuperior)
}

func (s *serverMaster) ID() string {
	if s == nil {
		return ""
	}
	return s.id
}

func (s *serverMaster) TryConnect(hostID string) bool {
	if s.url == "" {
		return false
	}
	if hostID == s.id {
		return false
	}
	if time.Now().Sub(s.lastConnected) < time.Second*5 {
		return false
	}
	if s.redirect != nil && time.Now().Sub(s.lastConnected) < time.Second*60 {
		return false
	}
	if s.failedCount > 5 && time.Now().Sub(s.lastConnected) < time.Minute {
		return false
	}
	return true
}

func (s *serverMaster) Alive() bool {
	if s == nil {
		return false
	}
	return s.conn != nil && s.conn.Alive()
}

type Masters []*serverMaster

func (m Masters) Search(url string) *serverMaster {
	for _, c := range m {
		if c.url == url || c.id == url {
			return c
		}
	}
	return nil
}

func newMasters(s []string, isSuperior bool) Masters {
	if len(s) == 0 {
		return nil
	}
	m := make([]*serverMaster, len(s))
	for i := range s {
		m[i] = &serverMaster{
			isSuperior: isSuperior,
			url:        s[i],
		}
	}
	return m
}

type Server struct {
	ctx          context.Context
	master       *serverMaster
	masterStable bool
	masterChan   chan *serverMaster
	// record the url address of the superior masters
	superiorMaster Masters
	// record the url address of the lateral masters
	lateralMaster         Masters
	lateralConns          map[string]Connection
	config                Config
	messageSerializer     *message.Serializer
	connections           sync.Map // key = the Connection ID.
	topics                trie.Trie
	mu                    sync.RWMutex // for topics.
	onConnectionListeners []ConnectionFunc
	//connectionPool        sync.Pool // sadly we can't make this because the websocket connection is live until is closed.
}

func New(cfg Config) *Server {
	cfg = cfg.Validate()
	s := &Server{
		config:                cfg,
		ctx:                   context.Background(),
		messageSerializer:     message.NewSerializer(cfg.EvtMessagePrefix),
		connections:           sync.Map{},
		topics:                trie.New(),
		mu:                    sync.RWMutex{},
		onConnectionListeners: make([]ConnectionFunc, 0, 5),
	}
	s.superiorMaster = newMasters(cfg.SuperiorMaster, true)
	s.lateralMaster = newMasters(cfg.LateralMaster, false)
	s.masterChan = make(chan *serverMaster, 5)
	if !cfg.EnableCluster {
		return s
	}
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			// 为空时取消master, 不为空时: 有连接则置为master, 无连接则尝试连接
			case c := <-s.masterChan:
				if c == nil {
					log.Debug().Msg(s.ID() + " try to connect to its master")
				} else {
					log.Debug().Msg(s.ID() + " try to connect to " + c.String())
				}
				// 仅在此处操作master
				if c == nil {
					// 置空
					if s.master.Alive() {
						s.master.conn.Close()
					}
					s.master = nil
				} else {
					// 仅在该处写入s.master
					if c.conn != nil {
						if s.master != nil {
							if s.master.url == c.url {
								// 发送重复
								break
							}
						}
						if s.master.Alive() {
							s.master.conn.Close()
						}
						s.master = c
						s.masterStable = true
						c.conn.OnDisconnect(func() {
							s.masterStable = false
							s.masterChan <- nil
						})
						log.Warn().Msgf("%s succeed to set {%s} as its master", s.ID(), c.String())
						break
					}
				}
				nms := make([]*serverMaster, 0, 10)
				if c != nil {
					nms = append(nms, c)
				}
				if len(s.lateralMaster) > 0 {
					nms = append(nms, s.lateralMaster...)
				}
				if len(s.superiorMaster) > 0 {
					nms = append(nms, s.superiorMaster...)
				}
				for _, c := range nms {
					if c.TryConnect(s.ID()) {
						if c.isSuperior {
							if s.connectSuperiorMasters(c) {
								s.masterChan <- c
								break
							}
						} else {
							ifConnected, ifBreak := s.connectLateralMaster(c)
							if ifBreak {
								break
							}
							if ifConnected {
								s.masterChan <- c
								break
							}
						}
					}
				}
			case <-ticker.C:
				if !s.masterStable {
					if s.master == nil || !s.master.Alive() {
						if len(s.superiorMaster) == 0 {
							stable := true
							for _, c := range s.lateralMaster {
								if c.id != s.ID() && c.failedCount <= 5 && c.redirect == nil {
									stable = false
								}
							}
							if stable {
								log.Warn().Msg("stable cluster")
							}
							s.masterStable = stable
						}
						if !s.masterStable {
							s.masterChan <- nil
						}
					}
				}
			}
		}
	}()
	time.Sleep(time.Duration(seed.Int63n(1000)) * time.Millisecond)
	s.masterChan <- nil
	return s
}

func (s *Server) getClusterInfo() string {
	res := ""
	for _, temp := range s.superiorMaster {
		res += fmt.Sprintf("%s,%s,%v\n", temp.url, temp.id, temp.isSuperior)
	}
	for _, temp := range s.lateralMaster {
		res += fmt.Sprintf("%s,%s,%v\n", temp.url, temp.id, temp.isSuperior)
	}
	return res
}

func (s *Server) updateClusterInfo(info string) {
	for _, l := range strings.Split(info, "\n") {
		if p := strings.Split(l, ","); len(p) == 3 {
			url, id, isSuperior := p[0], p[1], p[2]
			founded := false
			if isSuperior == "true" {
				for _, c := range s.superiorMaster {
					if c.id == id {
						founded = true
						break
					}
					if c.url == url {
						founded = true
						if c.id == "" {
							c.id = id
						}
					}
				}
			} else {
				for _, c := range s.lateralMaster {
					if c.id == id {
						founded = true
						break
					}
					if c.url == url {
						founded = true
						if c.id == "" {
							c.id = id
						}
					}
				}
			}
			if !founded {
				nc := &serverMaster{
					url: url,
					id:  id,
				}
				if isSuperior == "true" {
					nc.isSuperior = true
					s.superiorMaster = append(s.superiorMaster, nc)
				} else {
					s.lateralMaster = append(s.lateralMaster, nc)
				}
			}
		}
	}
}

func (s *Server) connectSuperiorMasters(c *serverMaster) bool {
	//conn := dialConn(s, c.url, 2)
	cha := make(chan bool, 1)
	select {
	case b := <-cha:
		return b
	case <-time.After(time.Second * 3):
		return false
	}
}

func (s *Server) connectLateralMaster(c *serverMaster) (ifConnected bool, ifBreak bool) {
	c.lastConnected = time.Now()
	conn := dialConn(s, c.url, 1)
	if conn == nil {
		c.failedCount++
		return
	}
	c.failedCount = 0
	cha := make(chan bool, 1)
	conn.OnConnect(func() {
		// 声明 自己是个平级节点 尝试建立节点连接
		conn.Echo(message.TopicLateral.String(), s.ID())
	})
	// 交换id
	conn.Subscribe(message.TopicLateral.String(), func(res string) {
		c.id = res
		conn.Echo(message.TopicClusterIps.String(), s.getClusterInfo())
	}).SetOnce()
	// 交换集群信息
	conn.Subscribe(message.TopicClusterIps.String(), func(res string) {
		// 同步已知节点数据
		s.updateClusterInfo(res)
		conn.Echo(message.TopicLateralRedirect.String(), s.ID())
	})
	// 请求是否跳转
	conn.Subscribe(message.TopicLateralRedirect.String(), func(res string) {
		if res != "" {
			conn.Close()
			temp := s.lateralMaster.Search(res)
			if temp == nil {
				log.Warn().Msgf("redirect new %s", res)
				temp = &serverMaster{
					isSuperior: false,
					url:        res,
				}
				s.lateralMaster = append(s.lateralMaster, temp)
			}
			c.redirect = temp
			if temp.id != s.ID() {
				log.Warn().Msgf("redirect %s", res)
				s.masterChan <- temp
				ifBreak = true
			}
			cha <- false
		} else if c.id == s.ID() {
			// 连接到本机
			conn.Close()
			cha <- false
		} else {
			_, ok := s.addConnection(conn)
			if !ok {
				conn.Close()
				cha <- false
				return
			}
			conn.OnDisconnect(func() {
				s.Disconnect(conn.ID())
			})
			c.conn = conn
			cha <- true
			for _, tempC := range s.lateralConns {
				tempC.Echo(message.TopicLateralRedirect.String(), c.url)
				tempC.Close()
			}
		}
	}).SetOnce()
	defer conn.OnDisconnect(func() {
		cha <- false
		conn = nil
	}).Cancel()
	select {
	case b := <-cha:
		ifConnected = b
		return
	case <-time.After(time.Second * 3):
		c.conn = nil
		log.HandlerErrs(conn.Close())
		log.Warn().Msg("close")
		return
	}
}

func (s *Server) ID() string {
	return s.config.ID
}

func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (Connection, error) {
	// create the new connection
	c, err := s.config.NewConn(s, w, r)
	if err != nil {
		return nil, err
	}
	for i := range s.onConnectionListeners {
		err = s.onConnectionListeners[i](c)
		if err != nil {
			c.Echo(message.TopicSysLog.String(), err.Error())
			c.Close()
			return nil, err
		}
	}
	return c, err
}

func (s *Server) addConnection(c interface{}) (interface{}, bool) {
	conn := c.(SampleConn)
	c, loaded := s.connections.LoadOrStore(conn.ID(), c)
	return c, !loaded
}

func (s *Server) GetConnection(id string) SampleConn {
	if conn, ok := s.getConnection(id).(SampleConn); ok {
		return conn
	}
	return nil
}

func (s *Server) getConnection(id string) interface{} {
	if temp, ok := s.connections.Load(id); ok {
		return temp
	}
	return nil
}

// OnConnection 当有新连接生成时触发
func (s *Server) OnConnection(cb ConnectionFunc) {
	s.onConnectionListeners = append(s.onConnectionListeners, cb)
}

func (s *Server) IsConnected(id string) bool {
	ok := s.getConnection(id)
	return ok != nil
}

func (s *Server) Subscribe(topic string, id string) {
	s.topics.AddSub(topic).AttachID(id)
}

func (s *Server) IsSubscribe(topic string, id string) bool {
	if m := s.topics.Match(topic); m != nil {
		return m.ExistID(id)
	}
	return false
}

func (s *Server) CancelSubscribe(topic, id string) {
	if m := s.topics.Match(topic); m != nil {
		m.DropID(id)
	}
}

func (s *Server) CancelAll(id string) {
	s.topics.DropAllSubID(id)
}

func (s *Server) LenConnections() (n int) {
	s.connections.Range(func(k, v interface{}) bool {
		n++
		return true
	})
	return
}

func (s *Server) GetConnections() []Connection {
	length := s.LenConnections()
	conns := make([]Connection, length, length)
	i := 0
	// second call of Range.
	s.connections.Range(func(k, v interface{}) bool {
		conn, ok := v.(*connection)
		if !ok {
			// if for some reason (should never happen), the value is not stored as *connection
			// then stop the iteration and don't continue insertion of the result connections
			// in order to avoid any issues while end-dev will try to iterate a nil entry.
			return false
		}
		conns[i] = conn
		i++
		return true
	})
	return conns
}

func (s *Server) GetConnectionsByTopic(topic string) []Connection {
	conns := make([]Connection, 0, 20)
	sub := s.topics.Match(topic)
	if sub == nil {
		return nil
	}
	ids := sub.IDs()
	for _, id := range ids {
		if temp, ok := s.connections.Load(id); ok {
			if conn, ok := temp.(*connection); ok {
				conns = append(conns, conn)
			}
		}
	}
	return conns
}

func (s *Server) Disconnect(id string) {
	if conn := s.GetConnection(id); conn != nil {
		s.CancelAll(id)
		s.connections.Delete(id)
		conn.Close()
	}
}

func (s *Server) Broadcast(topic string, msg []byte, connID string) error {
	t := s.topics.Match(topic)
	if t == nil {
		return errors.New("topic not exist")
	}
	return s.broadcast(t, msg, connID)
}

func (s *Server) broadcast(topic trie.Trie, msg []byte, connID string) error {
	if topic == nil {
		return errors.New("topic not exist")
	}
	if topic.IDs() == nil {
		return nil
	}
	var e error
	for _, id := range topic.IDs() {
		if id == connID {
			continue
		}
		if conn, ok := s.getConnection(id).(SampleConn); ok && conn != nil {
			_, e = conn.Write(msg)
			if e != nil {
				log.Info().Err(conn.Close()).Err(e).Msg("broadcast msg failed with " + id)
				e = nil
			}
		}
	}
	return nil
}

// 每个server 只允许有一个往出的连接 所以可以直接用server_id 做为conn_id
func dialConn(s *Server, url string, typ uint) *specialConn {
	c := &specialConn{server: s, typ: typ}
	c.Connection = client.New(&client.Config{
		Host:             url,
		ID:               s.ID(),
		EvtMessagePrefix: s.config.EvtMessagePrefix,
		PingPeriod:       s.config.PingPeriod,
		MaxMessageSize:   s.config.MaxMessageSize,
		BinaryMessages:   s.config.BinaryMessages,
		ReadBufferSize:   s.config.ReadBufferSize,
		WriteBufferSize:  s.config.WriteBufferSize,
	})
	connChan := make(chan bool, 1)
	c.Connection.OnConnect(func() {
		connChan <- true
	})
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Err(nil).Interface("panic", err).Msg("")
			}
		}()
		c.Wait()
		connChan <- false
		close(connChan)
	}()
	b := <-connChan
	if b {
		return c
	}
	return nil
}

type SampleConn interface {
	ID() string
	io.Writer
	io.Closer
	Server() *Server
	// 0: client  1: lateral 2: superior
	Type() uint
	Alive() bool
}

var _ SampleConn = &specialConn{}
var _ SampleConn = &connection{}

type specialConn struct {
	client.Connection
	server  *Server
	typ     uint
	latency int
}

// 0: client  1: lateral  2: superior
func (c *specialConn) Type() uint {
	return c.typ
}

func (c *specialConn) Server() *Server {
	return c.server
}
