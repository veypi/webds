package webds

import (
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"github.com/lightjiang/webds/trie"
)

type (
	publisher struct {
		conn  Connection
		topic message.Topic
		trie  trie.Trie
	}
)

var _ message.Publisher = &publisher{}

func newPublisher(c Connection, topic message.Topic) *publisher {
	if !message.IsPublicTopic(topic) {
		panic(message.ErrNotAllowedTopic)
	}
	return &publisher{conn: c, trie: c.Server().topics.AddSub(topic.String()), topic: topic}
}

func (e *publisher) Pub(data interface{}) {
	msg, err := e.conn.Server().messageSerializer.Serialize(e.topic, data)
	if err != nil {
		log.HandlerErrs(err)
		return
	}
	log.HandlerErrs(e.conn.Server().broadcast(e.trie, msg, e.conn.ID()))
}
