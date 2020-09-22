package webds

import (
	"context"
	"errors"
	"fmt"
	"github.com/veypi/utils"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/cfg"
	"github.com/veypi/webds/cluster"
	"github.com/veypi/webds/conn"
	"github.com/veypi/webds/core"
	"github.com/veypi/webds/message"
	"github.com/veypi/webds/trie"
	"net/http"
	"strings"
	"sync"
)

const (
	Version = "v0.2.9"
)

type (
	Webds      = core.Webds
	Connection = core.Connection
	Cluster    = core.Cluster
	Master     = core.Master
	Config     = cfg.Config
)

var (
	EncodeUrl = core.EncodeUrl
	DecodeUrl = core.DecodeUrl
)

var _ core.Webds = &webds{}

type webds struct {
	ctx                   context.Context
	cluster               core.Cluster
	cfg                   *cfg.Config
	connections           sync.Map // key = the Connection ID.
	topics                *trie.Trie
	addr                  string
	onConnectionListeners *message.SubscriberList
	onMsgListeners        map[string]*message.SubscriberList
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
		onMsgListeners:        make(map[string]*message.SubscriberList),
	}
	w.cfg.SetWebds(w)
	w.cluster = cluster.NewCluster(cfg.ID, cfg)
	if !cfg.EnableCluster {
		return w
	}
	for _, url := range cfg.ClusterMasters {
		w.cluster.AddUrl(url)
	}
	go w.cluster.Start()
	return w
}

func (s *webds) ID() string {
	return s.cfg.ID
}

func (s *webds) String() string {
	return fmt.Sprintf("%s(%s)", s.cfg.ID, s.addr)
}

func (s *webds) Cluster() core.Cluster {
	return s.cluster
}

func (s *webds) Upgrade(w http.ResponseWriter, r *http.Request) (core.Connection, error) {
	// create the new conn
	if !s.cfg.CheckOrigin(r) {
		return nil, ErrOrigin
	}
	c, err := conn.NewPassiveConn(w, r, s.cfg)
	if err != nil {
		return nil, err
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
func (s *webds) OnConnection(cb core.ConnectionFunc) *message.Subscriber {
	return s.onConnectionListeners.Add(func(data interface{}) {
		log.HandlerErrs(cb(data.(core.Connection)))
	})
}

// 当有连接接收到相关消息时触发
func (s *webds) OnMsg(t message.Topic, m message.Func) *message.Subscriber {
	if m == nil {
		return nil
	}
	if s.onMsgListeners[t.String()] == nil {
		s.onMsgListeners[t.String()] = message.NewSubscriberList()
	}
	return s.onMsgListeners[t.String()].Add(m)
}

func (s *webds) FireMsg(m *message.Message) {
	ts := message.NewTopic(m.Target).String()
	for t, listeners := range s.onMsgListeners {
		if listeners.Len() == 0 {
			continue
		}
		if strings.HasPrefix(ts, t) {
			listeners.Range(func(s *message.Subscriber) {
				s.Do(m)
			})
		}
	}
}

func (s *webds) Subscribe(topic string, id string) {
	if topic == "" || topic == "/" {
		log.Warn().Msgf("%s try to subscribe all: %s", id, utils.CallPath(1))
	}
	log.Debug().Msgf("%s subscribe %s", id, topic)
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

func (s *webds) GetConnectionsByTopic(topic string) []Connection {
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

func (s *webds) BroadcastMsg(topic string, msg interface{}) {
	t := message.NewTopic(topic)
	if !message.IsPublicTopic(t) {
		log.Warn().Msg(message.ErrNotAllowedTopic.Error())
		return
	}
	topic = t.String()
	m, err := message.Encode(t, msg)
	if err != nil {
		log.HandlerErrs(err)
		return
	}
	s.Broadcast(topic, m, "")

}

func (s *webds) Broadcast(topic string, msg []byte, connID string) {
	t := s.topics.LastMatch(topic)
	for {
		if t == nil {
			return
		}
		s.broadcast(t, msg, connID)
		t = t.Parent()
	}
}

func (s *webds) broadcast(topic *trie.Trie, msg []byte, connID string) {
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

func (s *webds) Listen(addr string) error {
	s.addr = addr
	return http.ListenAndServe(addr, s)
}

func (s *webds) AutoListen() error {
	min := s.cfg.ClusterPortMin
	max := s.cfg.ClusterPortMax
	index := min
	for {
		s.addr = fmt.Sprintf(":%d", index)
		_ = s.Listen(s.addr)
		index++
		if index > max {
			break
		}
	}
	return errors.New("no valid host")
}

func (s *webds) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := s.Upgrade(w, r)
	if err != nil {
		if err.Error() == "id is not exist" {
			return
		}
		log.HandlerErrs(err)
		return
	}
	defer c.Close()
	log.HandlerErrs(c.Wait())
}

func (s *webds) Topics() *trie.Trie {
	return s.topics
}
