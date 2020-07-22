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
	AddConnection(Connection) bool
	DelConnection(id string)
	GetConnection(id string) Connection
	Range(func(id string, c Connection) bool)
	Broadcast(topic string, msg []byte, id string)
	Subscribe(topic string, id string)
	CancelSubscribe(topic string, id string)
	CancelAll(id string)
	Topics() *trie.Trie
	Cluster() Cluster
	ID() string
}
