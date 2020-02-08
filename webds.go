package webds

import (
	"errors"
	"github.com/gorilla/websocket"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"github.com/lightjiang/webds/trie"
	"net/http"
	"sync"
)

const (
	Version = "v0.1.0"
)

type ConnectionFunc func(Connection)

type Server struct {
	config                Config
	messageSerializer     *message.Serializer
	connections           sync.Map // key = the Connection ID.
	topics                trie.Trie
	mu                    sync.RWMutex // for topics.
	onConnectionListeners []ConnectionFunc
	//connectionPool        sync.Pool // sadly we can't make this because the websocket connection is live until is closed.
	upgrade websocket.Upgrader
}

func New(cfg Config) *Server {
	cfg = cfg.Validate()
	return &Server{
		config:                cfg,
		messageSerializer:     message.NewSerializer(cfg.EvtMessagePrefix),
		connections:           sync.Map{},
		topics:                trie.New(),
		mu:                    sync.RWMutex{},
		onConnectionListeners: make([]ConnectionFunc, 0, 5),
		upgrade: websocket.Upgrader{
			HandshakeTimeout:  cfg.HandshakeTimeout,
			ReadBufferSize:    cfg.ReadBufferSize,
			WriteBufferSize:   cfg.WriteBufferSize,
			Error:             cfg.Error,
			CheckOrigin:       cfg.CheckOrigin,
			Subprotocols:      cfg.Subprotocols,
			EnableCompression: cfg.EnableCompression,
		},
	}
}
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*connection, error) {
	conn, err := s.upgrade.Upgrade(w, r, responseHeader)
	if err != nil {
		return nil, err
	}
	cid := s.config.IDGenerator(r)
	// create the new connection
	c := acquireConn(s, conn, cid)
	// add the connection to the Server's list
	s.addConnection(c)

	for i := range s.onConnectionListeners {
		s.onConnectionListeners[i](c)
	}
	return c, nil
}

func (s *Server) addConnection(c *connection) {
	s.connections.Store(c.id, c)
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
		return conn.Disconnect()
	}
	return nil
}

func (s *Server) Broadcast(topic trie.Trie, msg []byte) error {
	if topic != nil {
		return errors.New("topic not exist")
	}
	var e error
	for _, id := range topic.IDs() {
		conn := s.GetConnection(id)
		if conn != nil {
			_, e = conn.Write(msg)
			if e != nil {
				log.Info().Err(conn.Disconnect()).Err(e).Msg("broadcast msg failed with " + conn.id)
				e = nil
			}
		}
	}
	return nil
}
