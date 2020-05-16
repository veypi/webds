package core

import "github.com/lightjiang/webds/trie"

type Webds interface {
	AddConnection(Connection) bool
	DelConnection(id string)
	GetConnection(id string) Connection
	Range(func(id string, c Connection) bool)
	Broadcast(topic string, msg []byte, id string) error
	Subscribe(topic string, id string)
	CancelSubscribe(topic string, id string)
	CancelAll(id string)
	Topics() trie.Trie
	Cluster() Cluster
	ID() string
}
