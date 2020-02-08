package webds

import "github.com/lightjiang/webds/trie"

type (
	// Publisher is the message manager
	Publisher interface {
		// Pub sends a message on a particular topic
		Pub(interface{}) error
	}

	publisher struct {
		conn    *connection
		topic   trie.Trie
		absPath string
	}
)

var _ Publisher = &publisher{}

func newPublisher(c *connection, topic string) *publisher {
	return &publisher{conn: c, topic: c.server.topics.AddSub(topic), absPath: topic}
}

func (e *publisher) Pub(data interface{}) error {
	msg, err := e.conn.server.messageSerializer.Serialize(e.absPath, data)
	if err != nil {
		return err
	}
	return e.conn.server.Broadcast(e.topic, msg)
}
