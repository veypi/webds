package client

import (
	"bytes"
	"context"
	"errors"
	"github.com/lightjiang/utils"
	"io"
	"net/http"
	//"github.com/gorilla/websocket"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"nhooyr.io/websocket"
	"sync"
	"time"
)

const (
	// DefaultWebsocketPongTimeout 60 * time.Second
	DefaultWebsocketPongTimeout = 60 * time.Second
	// DefaultWebsocketPingPeriod (DefaultPongTimeout * 9) / 10
	DefaultWebsocketPingPeriod = (DefaultWebsocketPongTimeout * 9) / 10
	// DefaultWebsocketMaxMessageSize 10240000
	DefaultWebsocketMaxMessageSize = 10240000
	// DefaultWebsocketReadBufferSize 4096
	DefaultWebsocketReadBufferSize = 4096
	// DefaultWebsocketWriterBufferSize 4096
	DefaultWebsocketWriterBufferSize = 4096
	// DefaultEvtMessageKey is the default prefix of the underline websocket topics
	// that are being established under the hoods.
	//
	// Defaults to ":".
	// Last character of the prefix should be ':'.
	DefaultEvtMessageKey = "ws"
)

type (
	// DisconnectFunc is the callback which is fired when the ws client closed.
	DisconnectFunc = message.FuncBlank
	ConnectFunc    = message.FuncBlank
	// ErrorFunc is the callback which fires whenever an error occurs
	ErrorFunc = message.FuncError
	// Config websocket client config

	Connection interface {
		ID() string
		OnDisconnect(DisconnectFunc) message.Subscriber
		OnConnect(ConnectFunc) message.Subscriber
		OnError(ErrorFunc) message.Subscriber
		Subscribe(string, message.Func) message.Subscriber
		Publisher(string) func(interface{})
		Pub(string, interface{})
		Echo(string, interface{})
		Wait() error
		Close() error
		Alive() bool
		io.Writer
	}

	Config struct {
		// URL is the target
		Host string

		// ID used to create the client id
		ID string
		// EvtMessagePrefix prefix of the every message
		EvtMessagePrefix []byte
		// WriteTimeout time allowed to write a message to the connection.
		// 0 means no timeout.
		// Default value is 0
		// ReadTimeout time allowed to read a message from the connection.
		// 0 means no timeout.
		// Default value is 0
		// PingPeriod send ping messages to the connection within this period. Must be less than PongTimeout.
		// Default value is 60 *time.Second
		PingPeriod time.Duration
		// MaxMessageSize max message size allowed from connection.
		// Default value is 1024
		MaxMessageSize int64
		// BinaryMessages set it to true in order to denotes binary data messages instead of utf-8 text
		// compatible if you wanna use the Connection's EmitMessage to send a custom binary data to the client, like a native server-client communication.
		// Default value is false
		BinaryMessages bool
		// ReadBufferSize is the buffer size for the connection reader.
		// Default value is 4096
		ReadBufferSize int
		// WriteBufferSize is the buffer size for the connection writer.
		// Default value is 4096
		WriteBufferSize int
	}
)

// Validate validates the configuration
func (c *Config) Validate() {
	if c.PingPeriod <= 0 {
		c.PingPeriod = DefaultWebsocketPingPeriod
	}

	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = DefaultWebsocketMaxMessageSize
	}

	if c.ReadBufferSize <= 0 {
		c.ReadBufferSize = DefaultWebsocketReadBufferSize
	}

	if c.WriteBufferSize <= 0 {
		c.WriteBufferSize = DefaultWebsocketWriterBufferSize
	}

	if len(c.EvtMessagePrefix) == 0 {
		c.EvtMessagePrefix = []byte(DefaultEvtMessageKey)
	}
}

// New create a new websocket client
func New(conf *Config) Connection {
	c := &connection{
		ctx:                   context.Background(),
		messageType:           websocket.MessageBinary,
		onDisconnectListeners: message.NewSubscriberList(),
		onConnectListeners:    message.NewSubscriberList(),
		onErrorListeners:      message.NewSubscriberList(),
		onTopicListeners:      make(map[string]message.SubscriberList, 0),
	}
	conf.Validate()
	c.init(conf)
	return c
}

type connection struct {
	ctx               context.Context
	messageSerializer *message.Serializer
	id                string
	writerMu          sync.Mutex
	conn              *websocket.Conn
	config            *Config

	messageType  websocket.MessageType
	disconnected utils.SafeBool
	started      utils.SafeBool
	auth         utils.FastLocker

	onDisconnectListeners message.SubscriberList
	onConnectListeners    message.SubscriberList
	onErrorListeners      message.SubscriberList
	onTopicListeners      map[string]message.SubscriberList
}

func (c *connection) init(conf *Config) {
	c.config = conf
	c.messageSerializer = message.NewSerializer(c.config.EvtMessagePrefix)
	// will keep connecting to server
	if c.config.BinaryMessages {
		c.messageType = websocket.MessageBinary
	} else {
		c.messageType = websocket.MessageText
	}
	c.id = conf.ID
}
func (c *connection) write(websocketMessageType websocket.MessageType, data []byte) (int, error) {
	c.auth.Lock()
	defer c.auth.Unlock()
	if c.started.IfTrue() {
		return len(data), c.conn.Write(c.ctx, websocketMessageType, data)
	} else {
		return 0, errors.New("connection is not started. please call Wait() first")
	}
}

func (c *connection) Write(data []byte) (int, error) {
	return c.write(c.messageType, data)
}
func (c *connection) Close() error {
	if c.disconnected.SetTrue() {
		c.started.ForceSetFalse()
		c.fireDisconnect()
		err := c.conn.Close(websocket.StatusNormalClosure, "")
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
	return nil
}

func (c *connection) startConnect() error {
	log.Debug().Msgf("%s start connect %s", c.id, c.config.Host)
	conn, _, err := websocket.Dial(c.ctx, c.config.Host, &websocket.DialOptions{HTTPHeader: http.Header{"id": []string{c.id}}})
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *connection) startPing() {
	for {
		time.Sleep(c.config.PingPeriod)
		if c.disconnected.IfTrue() {
			break
		}
		err := c.conn.Ping(c.ctx)
		if err != nil {
			log.Error().Err(err).Msg("ping error")
			break
		}
	}
}

func (c *connection) startReader() error {
	c.conn.SetReadLimit(c.config.MaxMessageSize)
	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return nil
			}
			c.FireOnError(err)
			log.Error().Msg("read error: " + err.Error())
			return err
		}
		if len(data) > 0 {
			c.messageReceive(data)
		} else {
			return nil
		}
	}
}

func (c *connection) messageReceive(data []byte) {
	if bytes.HasPrefix(data, c.config.EvtMessagePrefix) {
		evt := c.messageSerializer.GetMsgTopic(data)
		if evt == nil {
			log.Warn().Err(message.InvalidMessage).Msg(string(data))
			return
		}
		customMessage, err := c.messageSerializer.Deserialize(data)
		if err != nil {
			log.Warn().Err(err).Msg(string(data))
			return
		}
		if message.IsSysTopic(evt) {
			switch evt.String() {
			case message.TopicSysLog.String():
				log.Info().Msgf("sys log : %s", customMessage)
				return
			case message.TopicAuth.String():
				if s, ok := customMessage.(string); ok && s == "pass" {
					c.auth.Unlock()
					log.Debug().Msgf("%s connected to %s succeed", c.id, c.config.Host)
					c.fireConnect()
				} else if s == "exit" {
					log.Info().Msg(s)
				} else {
					log.Warn().Interface("customMessage", customMessage).Msg("")
					log.HandlerErrs(c.Close())
				}
				return
			}
		}
		listeners, ok := c.onTopicListeners[evt.String()]
		if !ok || listeners.Len() == 0 {
			log.Warn().Msg("received data but no func handle it")
			return
		}
		listeners.Range(func(s message.Subscriber) {
			s.Do(customMessage)
		})
	} else {
		log.Warn().Err(message.InvalidMessage).Msg(string(data))
	}
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) OnDisconnect(cb DisconnectFunc) message.Subscriber {
	if c.disconnected.IfTrue() {
		// 如果已经断开连接则立即触发
		cb()
		return nil
	}
	return c.onDisconnectListeners.Add(cb)
}

func (c *connection) OnConnect(cb ConnectFunc) message.Subscriber {
	if c.started.IfTrue() {
		cb()
		return nil
	}
	return c.onConnectListeners.Add(cb)
}

func (c *connection) OnError(cb ErrorFunc) message.Subscriber {
	return c.onErrorListeners.Add(cb)
}

func (c *connection) FireOnError(err error) {
	c.onErrorListeners.Range(func(s message.Subscriber) {
		s.Do(err)
	})
}

func (c *connection) fireDisconnect() {
	c.onDisconnectListeners.Range(func(s message.Subscriber) {
		s.Do(nil)
	})
}

func (c *connection) fireConnect() {
	for v := range c.onTopicListeners {
		c.subscribe(message.NewTopic(v))
	}
	c.onConnectListeners.Range(func(s message.Subscriber) {
		s.Do(nil)
	})
}

func (c *connection) Publisher(topic string) func(interface{}) {
	t := message.NewTopic(topic)
	return func(data interface{}) {
		if message.IsPublicTopic(t) {
			log.HandlerErrs(c.pub(t, data))
			return
		}
		log.HandlerErrs(message.ErrNotAllowedTopic)
	}
}

func (c *connection) Pub(topic string, data interface{}) {
	t := message.NewTopic(topic)
	if message.IsPublicTopic(t) {
		log.HandlerErrs(c.pub(t, data))
		return
	}
	log.HandlerErrs(message.ErrNotAllowedTopic)
}

func (c *connection) Echo(topic string, data interface{}) {
	t := message.NewTopic(topic)
	if message.IsPublicTopic(t) {
		log.HandlerErrs(message.ErrNotAllowedTopic)
		return
	}
	log.HandlerErrs(c.pub(t, data))
}

func (c *connection) pub(topic message.Topic, data interface{}) error {
	m, err := c.messageSerializer.Serialize(topic, data)
	if err != nil {
		return err
	}
	_, err = c.Write(m)
	return err
}

func (c *connection) Subscribe(topic string, cb message.Func) message.Subscriber {
	t := message.NewTopic(topic)
	if c.onTopicListeners[t.String()] == nil {
		c.onTopicListeners[t.String()] = message.NewSubscriberList()
		c.subscribe(t)
	}
	return c.onTopicListeners[t.String()].Add(cb)
}

func (c *connection) subscribe(topic message.Topic) {
	if c.started.IfTrue() && message.IsPublicTopic(topic) {
		log.HandlerErrs(c.pub(message.TopicSubscribe, topic.String()))
	}
}

// Wait starts the ping and the messages reader,
// it's named as "Wait" because it should be called LAST,
// after the "On" topics IF server's `Upgrade` is used,
// otherwise you don't have to call it because the `Handler()` does it automatically.
func (c *connection) Wait() error {
	if c.started.SetTrue() {
		// 连接成功才有权写入数据
		c.auth.Lock()
		err := c.startConnect()
		if err != nil {
			return err
		}
		defer func() {
			log.HandlerErrs(c.Close())
		}()
		// start the ping
		go c.startPing()

		// start the messages reader
		err = c.startReader()
		return err
	}
	return nil
}

func (c *connection) Alive() bool {
	if c == nil {
		return false
	}
	if c.started.IfTrue() && !c.disconnected.IfTrue() {
		return true
	}
	return false
}
