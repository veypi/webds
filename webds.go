package webds

import (
	"context"
	"errors"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"github.com/lightjiang/webds/trie"
	"net/http"
	"sync"
)

const (
	Version = "v0.2.2"
)

type ConnectionFunc func(Connection) error

type Server struct {
	ctx                   context.Context
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
	return &Server{
		config:                cfg,
		ctx:                   context.Background(),
		messageSerializer:     message.NewSerializer(cfg.EvtMessagePrefix),
		connections:           sync.Map{},
		topics:                trie.New(),
		mu:                    sync.RWMutex{},
		onConnectionListeners: make([]ConnectionFunc, 0, 5),
	}
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

func (s *Server) addConnection(c *connection) (*connection, bool) {
	conn, loaded := s.connections.LoadOrStore(c.id, c)
	return conn.(*connection), !loaded
}

func (s *Server) GetConnection(id string) *connection {
	if temp, ok := s.connections.Load(id); ok {
		if conn, ok := temp.(*connection); ok {
			return conn
		}
	}
	return nil
}

// OnConnection 当有新连接生成时触发
func (s *Server) OnConnection(cb ConnectionFunc) {
	s.onConnectionListeners = append(s.onConnectionListeners, cb)
}

func (s *Server) IsConnected(id string) bool {
	ok := s.GetConnection(id)
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

func (s *Server) Disconnect(id string) error {
	if conn := s.GetConnection(id); conn != nil {
		return conn.Disconnect(nil)
	}
	return nil
}

func (s *Server) Broadcast(topic string, msg []byte) error {
	t := s.topics.Match(topic)
	if t == nil {
		return errors.New("topic not exist")
	}
	return s.broadcast(t, msg)
}

func (s *Server) broadcast(topic trie.Trie, msg []byte) error {
	if topic == nil {
		return errors.New("topic not exist")
	}
	if topic.IDs() == nil {
		return nil
	}
	var e error
	for _, id := range topic.IDs() {
		conn := s.GetConnection(id)
		if conn != nil {
			_, e = conn.Write(msg)
			if e != nil {
				log.Info().Err(conn.Disconnect(e)).Err(e).Msg("broadcast msg failed with " + conn.id)
				e = nil
			}
		}
	}
	return nil
}
