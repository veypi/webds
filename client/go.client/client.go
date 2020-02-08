package client

import (
	"bytes"
	"github.com/gorilla/websocket"
	"github.com/kataras/golog"
	"github.com/lightjiang/webds/message"
	"net"
	"net/url"
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
	// DefaultEvtMessageKey is the default prefix of the underline websocket events
	// that are being established under the hoods.
	//
	// Defaults to "go-websocket-message:".
	// Last character of the prefix should be ':'.
	DefaultEvtMessageKey = "ws:"
)

type (
	// DisconnectFunc is the callback which is fired when the ws client closed.
	DisconnectFunc func()
	ConnectFunc    func()
	// ErrorFunc is the callback which fires whenever an error occurs
	ErrorFunc (func(error))
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
		OnPing(PingFunc)
		OnPong(PongFunc)
		FireOnError(err error)
		OnMessage(NativeMessageFunc)
		On(string, MessageFunc)
		Wait() error
		Close() error
		Emit(string, interface{}) error
		EmitMessage([]byte) error
	}

	Config struct {
		Key string
		// URL is the target
		URL string

		Path string
		// ID used to create the client id
		ID string
		// EvtMessagePrefix prefix of the every message
		EvtMessagePrefix []byte
		// WriteTimeout time allowed to write a message to the connection.
		// 0 means no timeout.
		// Default value is 0
		WriteTimeout time.Duration
		// ReadTimeout time allowed to read a message from the connection.
		// 0 means no timeout.
		// Default value is 0
		ReadTimeout time.Duration
		// PongTimeout allowed to read the next pong message from the connection.
		// Default value is 60 * time.Second
		PongTimeout time.Duration
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
	// 0 means no timeout.
	if c.WriteTimeout < 0 {
		c.WriteTimeout = DefaultWebsocketWriteTimeout
	}

	if c.ReadTimeout < 0 {
		c.ReadTimeout = DefaultWebsocketReadTimeout
	}

	if c.PongTimeout < 0 {
		c.PongTimeout = DefaultWebsocketPongTimeout
	}

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
		messageType:              websocket.TextMessage,
		onDisconnectListeners:    make([]DisconnectFunc, 0),
		onConnectListeners:       make([]ConnectFunc, 0),
		onErrorListeners:         make([]ErrorFunc, 0),
		onNativeMessageListeners: make([]NativeMessageFunc, 0),
		onEventListeners:         make(map[string][]MessageFunc, 0),
		onPongListeners:          make([]PongFunc, 0),
		onPingListeners:          make([]PingFunc, 0),
	}
	conf.Validate()
	c.init(conf)
	return c
}

type connection struct {
	messageSerializer *message.Serializer
	id                string
	writerMu          sync.Mutex
	conn              *websocket.Conn
	config            *Config

	messageType              int
	disconnected             bool
	onDisconnectListeners    []DisconnectFunc
	onConnectListeners       []ConnectFunc
	onErrorListeners         []ErrorFunc
	onPingListeners          []PingFunc
	onPongListeners          []PongFunc
	onNativeMessageListeners []NativeMessageFunc
	onEventListeners         map[string][]MessageFunc
	started                  bool
}

func (c *connection) init(conf *Config) {
	c.config = conf
	c.messageSerializer = message.NewSerializer(c.config.EvtMessagePrefix)
	// will keep connecting to server
	if c.config.BinaryMessages {
		c.messageType = websocket.BinaryMessage
	}
	c.id = conf.ID
}
func (c *connection) write(websocketMessageType int, data []byte) error {
	c.writerMu.Lock()
	if timeout := c.config.WriteTimeout; timeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	err := c.conn.WriteMessage(websocketMessageType, data)
	c.writerMu.Unlock()
	if err != nil {
		c.Close()
	}
	return err
}

func (c *connection) writeDefault(data []byte) error {
	return c.write(c.messageType, data)
}

func (c *connection) Close() error {
	defer func() {
		c.started = false
		c.disconnected = true
		c.fireDisconnect()
	}()
	if c == nil || c.disconnected {
		return nil
	}
	return c.conn.Close()
}

func (c *connection) startConnect() {
	for {
		u := url.URL{Scheme: "ws", Host: c.config.URL, Path: c.config.Path + "/" + c.id + "/" + c.config.Key}
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			golog.Warnf("connected failed %s . %s", u.String(), err.Error())
			time.Sleep(10 * time.Second)
		} else {
			c.conn = conn
			golog.Infof("connect %s succeed.", u.String())
			c.fireConnect()
			break
		}
	}
}

func (c *connection) startPinger() {
	pingHandler := func(s string) error {
		err := c.conn.WriteControl(websocket.PongMessage, []byte(s), time.Now().Add(time.Second))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Temporary() {
			return nil
		}
		return err
	}
	c.conn.SetPingHandler(pingHandler)
	c.conn.SetPongHandler(func(s string) error {
		go c.fireOnPong(s)
		return nil
	})
	go func() {
		for {
			time.Sleep(c.config.PingPeriod)
			if c.disconnected {
				break
			}
			c.fireOnPing()
			err := c.write(websocket.PingMessage, []byte{})
			if err != nil {
				golog.Errorf("ping error: %s", err.Error())
				break
			}
		}
	}()
}

func (c *connection) startReader() error {
	c.conn.SetReadLimit(c.config.MaxMessageSize)
	defer func() {
		c.conn.Close()
	}()

	hasReadTimeout := c.config.ReadTimeout > 0

	for {
		if hasReadTimeout {
			c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				c.FireOnError(err)
				golog.Errorf("read error: %s", err.Error())
				return err
			}
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
		listeners, ok := c.onEventListeners[string(evt)]
		if !ok || len(listeners) == 0 {
			golog.Warnf("received data but no func handle it: %s", evt)
			return
		}
		message, err := c.messageSerializer.Deserialize(evt.String(), data)
		if message == nil || err != nil {
			golog.Warnf("received blank data or deserialize failed for %v", err)
			return
		}
		golog.Debugf("on evt:%s:%s", evt, message)
		for i := range listeners {
			if fn, ok := listeners[i].(func()); ok { // its a simple func(){} callback
				fn()
			} else if fnString, ok := listeners[i].(func(string)); ok {

				if msgString, is := message.(string); is {
					fnString(msgString)
				} else if msgInt, is := message.(int); is {
					// here if server side waiting for string but client side sent an int, just convert this int to a string
					fnString(strconv.Itoa(msgInt))
				}

			} else if fnInt, ok := listeners[i].(func(int)); ok {
				fnInt(message.(int))
			} else if fnBool, ok := listeners[i].(func(bool)); ok {
				fnBool(message.(bool))
			} else if fnBytes, ok := listeners[i].(func([]byte)); ok {
				fnBytes(message.([]byte))
			} else {
				listeners[i].(func(interface{}))(message)
			}
		}
	} else {
		golog.Debugf("on message: " + string(data))
		// it's native websocket message
		for i := range c.onNativeMessageListeners {
			c.onNativeMessageListeners[i](data)
		}
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

func (c *connection) OnPing(cb PingFunc) {
	c.onPingListeners = append(c.onPingListeners, cb)
}

func (c *connection) OnPong(cb PongFunc) {
	c.onPongListeners = append(c.onPongListeners, cb)
}

func (c *connection) FireOnError(err error) {
	golog.Debugf("on error: %v", err)
	for _, cb := range c.onErrorListeners {
		cb(err)
	}
}

func (c *connection) fireDisconnect() {
	golog.Debugf("disconnected.")
	for _, fc := range c.onDisconnectListeners {
		fc()
	}
}

func (c *connection) fireConnect() {
	golog.Debugf("connected.")
	for _, fc := range c.onConnectListeners {
		fc()
	}
}

func (c *connection) fireOnPing() {
	// fire the onPingListeners
	for i := range c.onPingListeners {
		c.onPingListeners[i]()
	}
}

func (c *connection) fireOnPong(s string) {
	// fire the onPongListeners
	for i := range c.onPongListeners {
		c.onPongListeners[i](s)
	}
}

func (c *connection) EmitMessage(nativeMessage []byte) error {
	return c.writeDefault(nativeMessage)
}

func (c *connection) Emit(event string, message interface{}) error {
	m, err := c.messageSerializer.Serialize(event, message)
	if err != nil {
		return err
	}
	return c.writeDefault(m)
}

func (c *connection) OnMessage(cb NativeMessageFunc) {
	c.onNativeMessageListeners = append(c.onNativeMessageListeners, cb)
}

func (c *connection) On(event string, cb MessageFunc) {
	if c.onEventListeners[event] == nil {
		c.onEventListeners[event] = make([]MessageFunc, 0)
	}

	c.onEventListeners[event] = append(c.onEventListeners[event], cb)
}

// Wait starts the pinger and the messages reader,
// it's named as "Wait" because it should be called LAST,
// after the "On" events IF server's `Upgrade` is used,
// otherise you don't have to call it because the `Handler()` does it automatically.
func (c *connection) Wait() error {
	if c.started {
		return nil
	}
	c.startConnect()
	c.started = true
	// start the ping
	c.startPinger()

	// start the messages reader
	err := c.startReader()
	c.Close()
	return err
}
