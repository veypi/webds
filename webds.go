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
	"sync"
	"time"
)

const (
	Version = "v0.2.3"
)

var seed = rand.New(rand.NewSource(time.Now().Unix()))

type ConnectionFunc func(Connection) error

type serverMaster struct {
	isSuperior    bool
	url           string
	id            string
	latency       int
	lastConnected time.Time
	connIn        *connection
	connOut       *specialConn
}

func (s *serverMaster) String() string {
	return fmt.Sprintf("%s;%s;%v", s.url, s.id, s.isSuperior)
}

func (s *serverMaster) Alive(isOut bool) bool {
	if s == nil {
		return false
	}
	if isOut {
		return s.connOut != nil && s.connOut.Alive()
	} else {
		return s.connIn != nil && s.connIn.Alive()
	}
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
	ctx        context.Context
	master     *serverMaster
	masterChan chan *serverMaster
	// record the url address of the superior masters
	superiorMaster Masters
	// record the url address of the lateral masters
	lateralMaster         Masters
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
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		changed := false
		for {
			select {
			// 为空时取消master, 不为空时: 有连接则置为master, 无连接则尝试连接
			case c := <-s.masterChan:
				log.Warn().Msgf("%s: %+v", s.ID(), c)
				// 仅在此处操作master
				if c == nil {
					if s.master != nil {
						if s.master.connOut != nil {
							s.master.connOut.Disconnect(nil)
						}
						s.master = nil
					}
				} else {
					if c.connOut != nil {
						if s.master != nil {
							if s.master.url == c.url {
								// 发送重复
								break
							}
						}
						if s.master != nil && s.master.connOut != nil {
							s.master.connOut.Disconnect(nil)
						}
						s.master = c
						changed = true
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
					if c.connIn == nil && c.url != "" && time.Now().Sub(c.lastConnected) > time.Second*5 {
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
				if s.master == nil {
					s.masterChan <- nil
				}
			}
			if changed && s.master != nil {
				changed = false
			}
		}
	}()
	time.Sleep(time.Duration(seed.Int63n(1000)) * time.Millisecond)
	s.masterChan <- nil
	return s
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
		return
	}
	cha := make(chan bool, 1)
	conn.OnConnect(func() {
		// 声明 自己是个平级节点 尝试建立节点连接
		conn.Pub(message.TopicLateral.String(), s.ID())
	})
	conn.Subscribe(message.TopicLateralIps.String(), func(res []byte) {
		// 同步已知节点数据
		log.Warn().Msg(string(res))
	})
	conn.Subscribe(message.TopicLateral.String(), func(res string) {
		// res 返回 非空字符串(对方id) 则建立连接成功
		// 避免自己连接自己
		if res == "" || res == s.ID() {
			c.id = res
			conn.Disconnect(nil)
			cha <- false
			return
		}
		if !conn.Alive() {
			return
		}
		_, ok := s.addConnection(conn)
		if !ok {
			conn.Disconnect(nil)
			cha <- false
			return
		}
		conn.OnDisconnect(func() {
			s.Disconnect(conn.ID())
		})
		c.connOut = conn
		c.id = res
		cha <- true
	})
	conn.Subscribe(message.TopicLateralRedirect.String(), func(res string) {
		if res != "" {
			conn.Disconnect(nil)
			temp := s.lateralMaster.Search(res)
			if temp == nil {
				log.Warn().Msgf("redirect new %s", res)
				temp = &serverMaster{
					isSuperior: false,
					url:        res,
				}
				s.lateralMaster = append(s.lateralMaster, temp)
			}
			log.Warn().Msgf("redirect %s", res)
			s.masterChan <- temp
			ifBreak = true
			cha <- false
		}
	})
	conn.OnDisconnect(func() {
		cha <- false
		conn = nil
	})
	select {
	case b := <-cha:
		ifConnected = b
		return
	case <-time.After(time.Second * 3):
		c.connOut = nil
		log.HandlerErrs(conn.Disconnect(nil))
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
			c.Disconnect(nil)
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
		conn.Disconnect(nil)
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
				log.Info().Err(conn.Disconnect(e)).Err(e).Msg("broadcast msg failed with " + id)
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
		log.HandlerErrs(c.Wait())
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
	Server() *Server
	Type() uint
	Disconnect(error) error
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

func (c *specialConn) Disconnect(err error) error {
	return c.Close()
}

func (c *specialConn) Server() *Server {
	return c.server
}
