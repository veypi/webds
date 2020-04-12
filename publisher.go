package webds

import (
	"github.com/lightjiang/webds/message"
	"github.com/lightjiang/webds/trie"
)

type (
	// Publisher is the message manager
	Publisher interface {
		// Pub sends a message on a particular topic
		Pub(interface{}) error
	}

	publisher struct {
		conn  Connection
		topic message.Topic
		trie  trie.Trie
	}
)

var _ Publisher = &publisher{}

func newPublisher(c Connection, topic message.Topic) *publisher {
	return &publisher{conn: c, trie: c.Server().topics.AddSub(topic.String()), topic: topic}
}

func (e *publisher) Pub(data interface{}) error {
	msg, err := e.conn.Server().messageSerializer.Serialize(e.topic, data)
	if err != nil {
		return err
	}
	return e.conn.Server().broadcast(e.trie, msg, e.conn.ID())
}
