package webds

import (
	"context"
	"errors"
	"fmt"
	"github.com/lightjiang/utils"
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

type ConnectionFunc func(Connection) error

type serverMaster struct {
	isSuperior bool
	url        string
	id         string
	latency    int
	connIn     *connection
	connOut    *specialConn
}

func (s *serverMaster) String() string {
	return fmt.Sprintf("%s;%s;%v", s.url, s.id, s.isSuperior)
}

func newMasters(s []string, isSuperior bool) []*serverMaster {
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
	ctx    context.Context
	master *serverMaster
	// record the url address of the superior masters
	superiorMaster []*serverMaster
	// record the url address of the lateral masters
	lateralMaster         []*serverMaster
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
	go func() {
		for {
			if s.master == nil {
				isSync := false
				// 检测有没有没有连入的同级节点
				for _, c := range s.lateralMaster {
					if c.connIn == nil {
						isSync = true
					}
				}
				// 看有没有父级节点可以连接
				if len(s.superiorMaster) != 0 {
					isSync = true
				}
				if isSync {
					s.syncMasters()
				}
			}
			time.Sleep(time.Second)
		}
	}()
	return s
}

func (s *Server) syncMasters() {
	log.Warn().Msgf("%d", rand.Int31n(1000))
	time.Sleep(time.Duration(rand.Int63n(1000)) * time.Millisecond)
	log.Warn().Msgf("\nlateral: \n%+v\n superior: \n%+v", s.lateralMaster, s.superiorMaster)
	for _, c := range s.lateralMaster {
		if s.master != nil {
			return
		}
		if c.connIn != nil {
			continue
		}
		temp := s.connectMaster(c)
		if temp != nil {
			break
		}
	}
}

func (s *Server) connectMaster(c *serverMaster) *serverMaster {
	conn := dialConn(s, c.url, 1)
	cha := make(chan bool, 1)
	conn.OnConnect(func() {
		conn.Pub(message.TopicLateral.String(), s.ID())
	})
	conn.Subscribe(message.TopicLateralIps.String(), func(res []byte) {
		log.Warn().Msg(string(res))
	})
	conn.Subscribe(message.TopicLateral.String(), func(res string) {
		if res == "" {
			s.master = c
		}
		log.Warn().Msg(res)
	})
	select {
	case <-cha:
	case <-time.After(time.Second * 5):
		log.HandlerErrs(conn.Disconnect(nil))
	}
	return nil
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
	_, ok := c.server.addConnection(c)
	if !ok {
		return nil
	}
	go func() {
		err := c.Wait()
		log.HandlerErrs(c.Disconnect(err), err)
	}()
	return c
}

type SampleConn interface {
	ID() string
	io.Writer
	Server() *Server
	Type() uint
	Disconnect(error) error
}

var _ SampleConn = &specialConn{}
var _ SampleConn = &connection{}

type specialConn struct {
	client.Connection
	server       *Server
	typ          uint
	disconnected utils.SafeBool
	latency      int
}

// 0: client  1: lateral  2: superior
func (c *specialConn) Type() uint {
	return c.typ
}

func (c *specialConn) Disconnect(err error) error {
	if c.disconnected.SetTrue() {
		c.server.Disconnect(c.ID())
		return c.Close()
	}
	return nil
}

func (c *specialConn) Server() *Server {
	return c.server
}
