package core

import (
	"github.com/veypi/webds/message"
	"github.com/veypi/webds/trie"
	"net/http"
)

type ConnectionFunc func(Connection) error

type Webds interface {
	Upgrade(w http.ResponseWriter, r *http.Request) (Connection, error)
	OnConnection(cb ConnectionFunc) *message.Subscriber
	OnMsg(t message.Topic, m message.Func) *message.Subscriber
	FireMsg(m *message.Message)
	AddConnection(Connection) bool
	DelConnection(id string)
	GetConnection(id string) Connection
	GetConnectionsByTopic(topic string) []Connection
	Range(func(id string, c Connection) bool)
	Broadcast(topic string, msg []byte, id string)
	BroadcastMsg(topic string, msg interface{})
	Subscribe(topic string, id string)
	CancelSubscribe(topic string, id string)
	CancelAll(id string)
	Topics() *trie.Trie
	Cluster() Cluster
	ID() string
	String() string
	Listen(add string) error
	AutoListen() error
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}
