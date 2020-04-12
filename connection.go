package webds

import (
	"bytes"
	"context"
	"errors"
	"github.com/lightjiang/utils"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"io"
	"net/http"
	"nhooyr.io/websocket"
	"strings"
	"sync"
	"time"
)

type (
	// DisconnectFunc is the callback which is fired when a client/connection closed
	DisconnectFunc func()
	// ErrorFunc is the callback which fires whenever an error occurs
	ErrorFunc func(error)
	// Connection is the front-end API that you will use to communicate with the client side
	Connection interface {
		// ID returns the connection's identifier
		ID() string

		Request() *http.Request

		// Server returns the websocket server instance
		// which this connection is listening to.
		//
		// Its connection-relative operations are safe for use.
		Server() *Server

		// OnDisconnect registers a callback which is fired when this connection is closed by an error or manual
		OnDisconnect(DisconnectFunc)
		// OnError registers a callback which fires when this connection occurs an error
		OnError(ErrorFunc)
		// OnPing  registers a callback which fires on each ping
		// It does nothing more than firing the OnError listeners. It doesn't send anything to the client.
		FireOnError(err error)
		// To defines on which "topic" the server should send a message
		// returns an Publisher to send messages.
		// Broadcast to any node which subscribe this topic.
		// 发布给订阅主题的所有节点
		Publisher(string) Publisher
		// On registers a callback to a particular topic which is fired when a message to this topic is received
		// just for topic with prefix '/inner'
		// 注册某些话题的回调事件
		On(topic string, callback message.Func)

		// only send to the node of this conn
		Echo(topic string, data interface{}) error

		// subscribe registers this connection to a topic, if it doesn't exist then it creates a new.
		// One topic can have one or more connections. One connection can subscribe many topics.
		// All connections subscribe a topic specified by their `ID` automatically.
		// 代替节点订阅主题, 可以配置节点是否有权限订阅主题
		Subscribe(string)

		IsSubscribe(string) bool
		// Leave removes this connection entry from a room
		// Returns true if the connection has actually left from the particular room.
		CancelSubscribe(string)
		// Wait starts the ping and the messages reader,
		// it's named as "Wait" because it should be called LAST,
		// after the "Subscribe" events IF server's `Upgrade` is used,
		// otherwise you don't have to call it because the `Handler()` does it automatically.
		Wait()
		// Disconnect disconnects the client, close the underline websocket conn and removes it from the conn list
		// returns the error, if any, from the underline connection
		Disconnect(error) error

		// 0: client  1: lateral 2: superior
		Type() uint
	}

	connection struct {
		ctx       context.Context
		underline *websocket.Conn
		request   *http.Request
		id        string
		// 0: client  1: lateral 2: superior
		typ                   uint
		messageType           websocket.MessageType
		disconnected          utils.SafeBool
		onDisconnectListeners []DisconnectFunc
		onErrorListeners      []ErrorFunc
		onTopicListeners      map[string][]message.Func
		subscribed            []string
		// mu for self object write/read
		selfMU  sync.RWMutex
		started utils.SafeBool

		server *Server
	}
)

type RowConn = connection

var _ Connection = &connection{}

var connPool = sync.Pool{New: func() interface{} {
	return &connection{}
}}

func NewConn(s *Server, w http.ResponseWriter, r *http.Request) (Connection, error) {
	return acquireConn(s, w, r)
}

func acquireConn(s *Server, w http.ResponseWriter, r *http.Request) (*connection, error) {
	if !s.config.CheckOrigin(r) {
		return nil, ErrOrigin
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       nil,
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, err
	}
	c := connPool.Get().(*connection)
	c.id = s.config.IDGenerator(r)
	c.server = s
	c.underline = conn
	c.ctx = s.ctx
	c.request = r
	c.messageType = websocket.MessageText
	if s.config.BinaryMessages {
		c.messageType = websocket.MessageBinary
	}
	if c.id == "" {
		log.HandlerErrs(c.echo(message.TopicAuth, ErrID.Error()))
		log.HandlerErrs(c.underline.Close(websocket.StatusNormalClosure, ""))
		releaseConn(c)
		return nil, ErrID
	}
	_, ok := s.addConnection(c)
	if !ok {
		log.HandlerErrs(c.echo(message.TopicAuth, ErrDuplicatedClient.Error()))
		log.HandlerErrs(c.underline.Close(websocket.StatusNormalClosure, ""))
		releaseConn(c)
		return nil, ErrDuplicatedClient
	} else {
		log.HandlerErrs(c.echo(message.TopicAuth, "pass"))
	}
	c.onDisconnectListeners = make([]DisconnectFunc, 0)
	c.onErrorListeners = make([]ErrorFunc, 0)
	c.onTopicListeners = make(map[string][]message.Func, 0)
	c.subscribed = make([]string, 5)
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	return c, nil
}

func releaseConn(c *connection) {
	c.id = ""
	connPool.Put(c)
}

func (c *connection) Type() uint {
	return c.typ
}

// Write writes a raw websocket message with a specific type to the client
// used by ping messages and any CloseMessage types.
func (c *connection) write(websocketMessageType websocket.MessageType, data []byte) (int, error) {
	// for any-case the app tries to write from different goroutines,
	// we must protect them because they're reporting that as bug...
	ctx := c.ctx
	if writeTimeout := c.server.config.WriteTimeout; writeTimeout > 0 {
		// set the write deadline based on the configuration
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.server.config.WriteTimeout)
		defer cancel()
	}
	err := c.underline.Write(ctx, websocketMessageType, data)
	if err != nil {
		// if failed then the connection is off, fire the disconnect
		log.HandlerErrs(c.Disconnect(err))
		return 0, err
	}
	return len(data), nil
}

// writeDefault is the same as write but the message type is the configured by c.messageType
// if BinaryMessages is enabled then it's raw []byte as you expected to work with proto3
func (c *connection) Write(data []byte) (int, error) {
	return c.write(c.messageType, data)
}

func (c *connection) startPing() {
	var err error
	for {
		// using sleep avoids the ticker error that causes a memory leak
		time.Sleep(c.server.config.PingPeriod)
		if c.disconnected.IfTrue() {
			// verifies if already disconnected
			break
		}
		// try to ping the client, if failed then it disconnects
		err = c.underline.Ping(c.ctx)
		if err != nil {
			log.HandlerErrs(err)
			// must stop to exit the loop and finish the go routine
			break
		}
	}
}

func (c *connection) startReader() {
	conn := c.underline
	hasReadTimeout := c.server.config.ReadTimeout > 0

	conn.SetReadLimit(c.server.config.MaxMessageSize)

	defer func() {
		log.HandlerErrs(c.Disconnect(nil))
	}()

	for {
		var data []byte
		var err error
		if hasReadTimeout {
			ctx, cancel := context.WithTimeout(c.ctx, c.server.config.ReadTimeout)
			_, data, err = conn.Read(ctx)
			cancel()
		} else {
			_, data, err = conn.Read(c.ctx)
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			switch websocket.CloseStatus(err) {
			case websocket.StatusNormalClosure:
			case websocket.StatusGoingAway:
			default:
				log.HandlerErrs(err)
				c.FireOnError(err)
			}
			break
		} else {
			e := c.messageReceived(data)
			if e != nil {
				log.Warn().Msg(e.Error() + " : " + string(data))
			}
		}

	}

}

func (c *connection) messageReceived(data []byte) error {

	if bytes.HasPrefix(data, c.server.config.EvtMessagePrefix) {
		//it's a custom ws message
		topic := c.server.messageSerializer.GetMsgTopic(data)
		if message.IsSysTopic(topic) {
			if topic.Fragment(1) == "admin" && !strings.HasPrefix(c.request.RemoteAddr, "127.0.0.1") {
				log.Warn().Str("addr", c.request.RemoteAddr).Str("topic", topic.String()).Msg("receive invalid admin command")
				return nil
			}
			customMessage, _, err := c.server.messageSerializer.Deserialize(data)
			if err != nil {
				return err
			}
			switch topic.String() {
			case message.TopicSubscribe.String():
				// TODO: 权限验证
				if s, is := customMessage.(string); is && s != "" && s != "/" {
					c.Subscribe(s)
				}
			case message.TopicSubscribeAll.String():
				c.Subscribe("")
			case message.TopicCancel.String():
				if s, is := customMessage.(string); is {
					c.CancelSubscribe(s)
				}
			case message.TopicCancelAll.String():
				c.server.CancelAll(c.id)
			case message.TopicGetAllTopics.String():
				res := c.server.topics.String()
				if res != "" {
					res += "\n"
				}
				for i := range c.onTopicListeners {
					res += i + "\n"
				}
				log.HandlerErrs(c.echo(message.TopicGetAllTopics, res))
			case message.TopicGetAllNodes.String():
				res := ""
				c.server.connections.Range(func(key, value interface{}) bool {
					res += key.(string) + "\n"
					return true
				})
				log.HandlerErrs(c.echo(message.TopicGetAllNodes, res))
			case message.TopicStopNode.String():
				conn := c.server.getConnection(customMessage.(string))
				if conn, ok := conn.(Connection); ok && conn != nil {
					log.HandlerErrs(conn.Echo(message.TopicAuth.String(), "exit"), conn.Disconnect(nil))
				}
			default:
				log.Warn().Err(message.ErrUnformedMsg).Msg(string(data))
			}
		} else if message.IsPublicTopic(topic) {
			// TODO 广播权限验证
			// TODO 广播机制是否需要修改 现在为并发广播 是否需要集中发送到一个channel去进行广播以避免锁消耗
			_ = c.server.Broadcast(topic.String(), data, c.id)
		}
		// 触发本地的监听函数
		listeners, ok := c.onTopicListeners[topic.String()]
		if !ok || len(listeners) == 0 {
			return nil // if not listeners for this event exit from here
		}
		customMessage, msgType, err := c.server.messageSerializer.Deserialize(data)
		if err != nil {
			return err
		}
		for _, item := range listeners {
			doIt := false
			switch cb := item.(type) {
			case message.FuncBlank:
				cb()
				doIt = true
			case message.FuncInt:
				if msgType == message.MsgTypeInt {
					cb(customMessage.(int))
					doIt = true
				}
			case message.FuncString:
				if msgType == message.MsgTypeString {
					cb(customMessage.(string))
					doIt = true
				}
			case message.FuncBytes:
				if msgType == message.MsgTypeBytes || msgType == message.MsgTypeJSON {
					cb(customMessage.([]byte))
					doIt = true
				}
			case message.FuncBool:
				if msgType == message.MsgTypeBool {
					cb(customMessage.(bool))
					doIt = true
				}
			case message.FuncDefault:
				cb(customMessage)
				doIt = true
			default:
				log.Warn().Str("id", c.id).Str("topic", topic.String()).Msg(" register a invalid callback func")
				return nil
			}
			if !doIt {
				log.Warn().Str("id", c.id).Str("topic", topic.String()).Msg("receive a msg but not find appropriate func to handle it")
			}
		}
		return nil

	} else {
		return message.ErrUnformedMsg
	}
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) Server() *Server {
	return c.server
}

func (c *connection) fireDisconnect() {
	for i := range c.onDisconnectListeners {
		c.onDisconnectListeners[i]()
	}
}

func (c *connection) OnDisconnect(cb DisconnectFunc) {
	c.onDisconnectListeners = append(c.onDisconnectListeners, cb)
}

func (c *connection) OnError(cb ErrorFunc) {
	c.onErrorListeners = append(c.onErrorListeners, cb)
}

func (c *connection) FireOnError(err error) {
	for _, cb := range c.onErrorListeners {
		cb(err)
	}
}

func (c *connection) Echo(Topic string, data interface{}) error {
	topic := message.NewTopic(Topic)
	return c.echo(topic, data)
}

func (c *connection) echo(t message.Topic, data interface{}) error {
	msg, err := c.server.messageSerializer.Serialize(t, data)
	if err != nil {
		return err
	}
	_, err = c.Write(msg)
	return err
}

func (c *connection) On(Topic string, cb message.Func) {
	topic := message.NewTopic(Topic)
	switch cb.(type) {
	case message.FuncBlank:
	case message.FuncInt:
	case message.FuncString:
	case message.FuncBytes:
	case message.FuncDefault:
	case message.FuncBool:
	default:
		log.Warn().Str("id", c.id).Str("topic", Topic).Msg(" register a invalid callback func")
		return
	}
	if c.onTopicListeners[topic.String()] == nil {
		c.onTopicListeners[topic.String()] = make([]message.Func, 0)
	}
	c.onTopicListeners[topic.String()] = append(c.onTopicListeners[topic.String()], cb)
}

func (c *connection) Publisher(topic string) Publisher {
	return newPublisher(c, message.NewTopic(topic))
}

func (c *connection) Subscribe(topic string) {
	c.server.Subscribe(topic, c.id)
}

func (c *connection) IsSubscribe(topic string) bool {
	return c.server.IsSubscribe(topic, c.id)
}

func (c *connection) CancelSubscribe(topic string) {
	c.server.CancelSubscribe(topic, c.id)
}

func (c *connection) CancelAll() {
	c.server.CancelAll(c.id)
}

// Wait starts the ping and the messages reader,
// it's named as "Wait" because it should be called LAST,
// after the "On" events IF server's `Upgrade` is used,
// otherwise you don't have to call it because the `Handler()` does it automatically.
func (c *connection) Wait() {
	if c.started.SetTrue() {
		// start the ping
		go c.startPing()
		// start the messages reader
		c.startReader()
		return
	}
}

func (c *connection) Disconnect(reason error) error {
	if c.disconnected.SetTrue() {
		c.server.Disconnect(c.id)
		c.fireDisconnect()
		var err error
		if reason != nil {
			err = c.underline.Close(websocket.StatusAbnormalClosure, reason.Error())
		} else {
			err = c.underline.Close(websocket.StatusNormalClosure, "")
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if websocket.CloseStatus(err) == websocket.StatusGoingAway {
				return nil
			}
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				return err
			}
		}
		releaseConn(c)
		return nil
	}
	return nil
}

func (c *connection) Request() *http.Request {
	return c.request
}
