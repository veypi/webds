package webds

import (
	"context"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/cluster"
	"github.com/veypi/webds/conn"
	"github.com/veypi/webds/core"
	"github.com/veypi/webds/message"
	"github.com/veypi/webds/trie"
	"net/http"
	"sync"
)

const (
	Version = "v0.2.5"
)

type (
	Webds      = core.Webds
	Connection = core.Connection
	Cluster    = core.Cluster
	Master     = core.Master
)

var (
	EncodeUrl = core.EncodeUrl
	DecodeUrl = core.DecodeUrl
)

type ConnectionFunc func(core.Connection) error

var _ core.Webds = &webds{}

type webds struct {
	ctx                   context.Context
	cluster               core.Cluster
	cfg                   *Config
	connections           sync.Map // key = the Connection ID.
	topics                trie.Trie
	onConnectionListeners *message.SubscriberList
	//connectionPool        sync.Webds // sadly we can't make this because the websocket conn is live until is closed.
}

func New(cfg *Config) *webds {
	cfg.Validate()
	w := &webds{
		cfg:                   cfg,
		ctx:                   context.Background(),
		connections:           sync.Map{},
		topics:                trie.New(),
		onConnectionListeners: message.NewSubscriberList(),
	}
	w.cfg.webds = w
	if !cfg.EnableCluster {
		return w
	}
	w.cluster = cluster.NewCluster(cfg.ID, cfg)
	for _, url := range cfg.LateralMaster {
		w.cluster.AddUrl(url, 0)
	}
	for _, url := range cfg.SuperiorMaster {
		w.cluster.AddUrl(url, 1)
	}
	w.cluster.Start()
	return w
}

func (s *webds) ID() string {
	return s.cfg.ID
}
func (s *webds) Cluster() core.Cluster {
	return s.cluster
}

func (s *webds) Upgrade(w http.ResponseWriter, r *http.Request) (core.Connection, error) {
	// create the new conn
	if !s.cfg.CheckOrigin(r) {
		return nil, ErrOrigin
	}
	c, err := conn.NewPassiveConn(s.cfg.IDGenerator(r), w, r, s.cfg)
	if err != nil {
		return nil, err
	}
	if c.ID() == "" {
		err = ErrID
	}
	c.SetTargetID(s.ID())
	s.onConnectionListeners.Range(func(s *message.Subscriber) {
		s.Do(c)
	})
	return c, err
}

func (s *webds) AddConnection(c core.Connection) bool {
	_, loaded := s.connections.LoadOrStore(c.ID(), c)
	return !loaded
}

func (s *webds) GetConnection(id string) core.Connection {
	if temp, ok := s.connections.Load(id); ok && temp != nil {
		return temp.(core.Connection)
	}
	return nil
}

func (s *webds) DelConnection(id string) {
	if c := s.GetConnection(id); c != nil {
		s.CancelAll(id)
		s.connections.Delete(id)
		c.Close()
	}
}

func (s *webds) Range(fc func(id string, c core.Connection) bool) {
	s.connections.Range(func(key, value interface{}) bool {
		return fc(key.(string), value.(core.Connection))
	})
}

// OnConnection 当有新连接生成时触发
func (s *webds) OnConnection(cb ConnectionFunc) *message.Subscriber {
	return s.onConnectionListeners.Add(func(data interface{}) {
		log.HandlerErrs(cb(data.(core.Connection)))
	})
}

func (s *webds) Subscribe(topic string, id string) {
	s.topics.AddSub(topic).AttachID(id)
}

func (s *webds) IsSubscribe(topic string, id string) bool {
	if m := s.topics.Match(topic); m != nil {
		return m.ExistID(id)
	}
	return false
}

func (s *webds) CancelSubscribe(topic, id string) {
	if m := s.topics.Match(topic); m != nil {
		m.DropID(id)
	}
}

func (s *webds) CancelAll(id string) {
	s.topics.DropAllSubID(id)
}

func (s *webds) LenConnections() (n int) {
	s.connections.Range(func(k, v interface{}) bool {
		n++
		return true
	})
	return
}

func (s *webds) GetConnections() []core.Connection {
	length := s.LenConnections()
	conns := make([]core.Connection, length, length)
	i := 0
	// second call of Range.
	s.connections.Range(func(k, v interface{}) bool {
		c, ok := v.(core.Connection)
		if !ok {
			// if for some reason (should never happen), the value is not stored as *conn
			// then stop the iteration and don't continue insertion of the result connections
			// in order to avoid any issues while end-dev will try to iterate a nil entry.
			return false
		}
		conns[i] = c
		i++
		return true
	})
	return conns
}

func (s *webds) GetConnectionsByTopic(topic string) []core.Connection {
	conns := make([]core.Connection, 0, 20)
	sub := s.topics.Match(topic)
	if sub == nil {
		return nil
	}
	ids := sub.IDs()
	for _, id := range ids {
		if c := s.GetConnection(id); c != nil {
			conns = append(conns, c)
		}
	}
	return conns
}

func (s *webds) Broadcast(topic string, msg []byte, connID string) {
	t := s.topics.Match(topic)
	s.broadcast(t, msg, connID)
}

func (s *webds) broadcast(topic trie.Trie, msg []byte, connID string) {
	if topic == nil || topic.IDs() == nil {
		return
	}
	var e error
	for _, id := range topic.IDs() {
		if id == connID {
			continue
		}
		if c := s.GetConnection(id); c != nil {
			_, e = c.Write(msg)
			if e != nil {
				log.Info().Err(c.Close()).Err(e).Msg("broadcast msg failed with " + id)
				e = nil
			}
		}
	}
	return
}

func (s *webds) Topics() trie.Trie {
	return s.topics
}
