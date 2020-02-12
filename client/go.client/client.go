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
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultWebsocketWriteTimeout 0, no timeout
	DefaultWebsocketWriteTimeout = 0
	// DefaultWebsocketReadTimeout 0, no timeout
	DefaultWebsocketReadTimeout = 0
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
	DefaultEvtMessageKey = "ws:"
)

type (
	// DisconnectFunc is the callback which is fired when the ws client closed.
	DisconnectFunc func()
	ConnectFunc    func()
	// ErrorFunc is the callback which fires whenever an error occurs
	ErrorFunc func(error)
	// NativeMessageFunc is the callback for native websocket messages, receives one []byte parameter which is the raw client's message
	NativeMessageFunc func([]byte)
	// MessageFunc is the second argument to the Emitter's Emit functions.
	// A callback which should receives one parameter of type string, int, bool or any valid JSON/Go struct
	MessageFunc interface{}
	// PingFunc is the callback which fires each ping
	PingFunc func()
	// PongFunc is the callback which fires on pong message received
	PongFunc func(s string)
	// Config websocket client config

	Connection interface {
		ID() string
		OnDisconnect(DisconnectFunc)
		OnConnect(ConnectFunc)
		OnError(ErrorFunc)
		Subscribe(string, MessageFunc)
		Publisher(string) func(interface{}) error
		Pub(string, interface{}) error
		Echo(string, interface{}) error
		Wait() error
		Close() error
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
		onDisconnectListeners: make([]DisconnectFunc, 0),
		onConnectListeners:    make([]ConnectFunc, 0),
		onErrorListeners:      make([]ErrorFunc, 0),
		onTopicListeners:      make(map[string][]MessageFunc, 0),
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

	onDisconnectListeners []DisconnectFunc
	onConnectListeners    []ConnectFunc
	onErrorListeners      []ErrorFunc
	onTopicListeners      map[string][]MessageFunc
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
func (c *connection) write(websocketMessageType websocket.MessageType, data []byte) error {
	c.auth.Lock()
	defer c.auth.Unlock()
	if c.started.IfTrue() {
		return c.conn.Write(c.ctx, websocketMessageType, data)
	} else {
		return errors.New("connection is not started. please call Wait() first")
	}
}

func (c *connection) writeDefault(data []byte) error {
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
	log.Debug().Msg("start connect " + c.config.Host)
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
			log.Error().Err(err).Msg("read error")
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
		msg, err := c.messageSerializer.Deserialize(evt, data)
		if err != nil {
			log.Warn().Err(err).Msg(string(data))
			return
		}
		if message.IsSysTopic(evt) {
			switch evt.String() {
			case message.TopicSysLog.String():
				log.Info().Interface("msg", msg).Msg("")
				return
			case message.TopicAuth.String():
				if s, ok := msg.(string); ok && s == "pass" {
					c.auth.Unlock()
					log.Debug().Msg("auth pass")
					c.fireConnect()
				} else if s == "exit" {
					log.Info().Msg(s)
				} else {
					log.Warn().Interface("msg", msg).Msg("")
					log.HandlerErrs(c.Close())
				}
				return
			}
		}
		listeners, ok := c.onTopicListeners[evt.String()]
		if !ok || len(listeners) == 0 {
			log.Warn().Msg("received data but no func handle it")
			return
		}
		for i := range listeners {
			if fn, ok := listeners[i].(func()); ok { // its a simple func(){} callback
				fn()
			} else if fnString, ok := listeners[i].(func(string)); ok {

				if msgString, is := msg.(string); is {
					fnString(msgString)
				} else if msgInt, is := msg.(int); is {
					// here if server side waiting for string but client side sent an int, just convert this int to a string
					fnString(strconv.Itoa(msgInt))
				}

			} else if fnInt, ok := listeners[i].(func(int)); ok {
				fnInt(msg.(int))
			} else if fnBool, ok := listeners[i].(func(bool)); ok {
				fnBool(msg.(bool))
			} else if fnBytes, ok := listeners[i].(func([]byte)); ok {
				fnBytes(msg.([]byte))
			} else {
				listeners[i].(func(interface{}))(msg)
			}
		}
	} else {
		log.Warn().Err(message.InvalidMessage).Msg(string(data))
	}
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) OnDisconnect(cb DisconnectFunc) {
	c.onDisconnectListeners = append(c.onDisconnectListeners, cb)
}

func (c *connection) OnConnect(cb ConnectFunc) {
	c.onConnectListeners = append(c.onConnectListeners, cb)
}

func (c *connection) OnError(cb ErrorFunc) {
	c.onErrorListeners = append(c.onErrorListeners, cb)
}

func (c *connection) FireOnError(err error) {
	for _, cb := range c.onErrorListeners {
		cb(err)
	}
}

func (c *connection) fireDisconnect() {
	log.Debug().Msg("disconnected.")
	for _, fc := range c.onDisconnectListeners {
		fc()
	}
}

func (c *connection) fireConnect() {
	for v := range c.onTopicListeners {
		c.subscribe(message.NewTopic(v))
	}
	for _, fc := range c.onConnectListeners {
		fc()
	}
}

func (c *connection) Publisher(topic string) func(interface{}) error {
	t := message.NewTopic(topic)
	return func(data interface{}) error {
		if !message.IsPublicTopic(t) {
			return message.ErrNotAllowedTopic
		}
		return c.pub(t, data)
	}
}

func (c *connection) Pub(topic string, data interface{}) error {
	t := message.NewTopic(topic)
	return c.pub(t, data)
}

func (c *connection) Echo(topic string, data interface{}) error {
	t := message.NewTopic(topic)
	if !message.IsInnerTopic(t) {
		return message.ErrNotAllowedTopic
	}
	return c.pub(t, data)
}

func (c *connection) pub(topic message.Topic, data interface{}) error {
	m, err := c.messageSerializer.Serialize(topic, data)
	if err != nil {
		return err
	}
	return c.writeDefault(m)
}

func (c *connection) Subscribe(topic string, cb MessageFunc) {
	t := message.NewTopic(topic)
	if c.onTopicListeners[t.String()] == nil {
		c.onTopicListeners[t.String()] = make([]MessageFunc, 0)
		c.subscribe(t)
	}
	c.onTopicListeners[t.String()] = append(c.onTopicListeners[t.String()], cb)
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
		c.auth.Lock()
		err := c.startConnect()
		if err != nil {
			return err
		}
		defer func() {
			log.HandlerErrs(c.Close())
		}()
		// start the ping
		//go c.startPing()

		// start the messages reader
		err = c.startReader()
		return err
	}
	return nil
}
