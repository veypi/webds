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
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	// DisconnectFunc is the callback which is fired when a client/connection closed
	DisconnectFunc func()
	// LeaveRoomFunc is the callback which is fired when a client/connection leaves from any room.
	// This is called automatically when client/connection disconnected
	// (because websocket server automatically leaves from all joined rooms)
	LeaveRoomFunc func(roomName string)
	// ErrorFunc is the callback which fires whenever an error occurs
	ErrorFunc func(error)
	// A callback which should receives one parameter of type string, int, bool or any valid JSON/Go struct
	MessageFunc interface{}
	// PingFunc is the callback which fires each ping
	PingFunc func()
	// PongFunc is the callback which fires on pong message received
	PongFunc func()
	// Connection is the front-end API that you will use to communicate with the client side
	Connection interface {
		// ID returns the connection's identifier
		ID() string

		// Server returns the websocket server instance
		// which this connection is listening to.
		//
		// Its connection-relative operations are safe for use.
		Server() *Server

		// Write writes a raw websocket message with a specific type to the client
		// used by ping messages and any CloseMessage types.
		io.Writer

		// OnDisconnect registers a callback which is fired when this connection is closed by an error or manual
		OnDisconnect(DisconnectFunc)
		// OnError registers a callback which fires when this connection occurs an error
		OnError(ErrorFunc)
		// OnPing  registers a callback which fires on each ping
		// It does nothing more than firing the OnError listeners. It doesn't send anything to the client.
		FireOnError(err error)
		// To defines on which "topic" the server should send a message
		// returns an Publisher to send messages.
		Publisher(string) Publisher
		// On registers a callback to a particular topic which is fired when a message to this topic is received
		// just for topic with prefix '/inner'
		OnInner(string, MessageFunc)

		// subscribe registers this connection to a topic, if it doesn't exist then it creates a new.
		// One topic can have one or more connections. One connection can subscribe many topics.
		// All connections subscribe a topic specified by their `ID` automatically.
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
	}

	connection struct {
		ctx                   context.Context
		underline             *websocket.Conn
		request               *http.Request
		id                    string
		messageType           websocket.MessageType
		disconnected          utils.SafeBool
		onDisconnectListeners []DisconnectFunc
		onErrorListeners      []ErrorFunc
		onTopicListeners      map[string][]MessageFunc
		subscribed            []string
		// mu for self object write/read
		selfMU  sync.RWMutex
		started utils.SafeBool

		server *Server
	}
)

var _ Connection = &connection{}

var connPool = sync.Pool{New: func() interface{} {
	return &connection{}
}}

func acquireConn(s *Server, w http.ResponseWriter, r *http.Request) (*connection, error) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return nil, err
	}
	c := connPool.Get().(*connection)
	c.id = s.config.IDGenerator(r)
	c.server = s
	c.ctx = s.ctx
	c.request = r
	c.underline = conn
	c.messageType = websocket.MessageText
	if s.config.BinaryMessages {
		c.messageType = websocket.MessageBinary
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
	c.onTopicListeners = make(map[string][]MessageFunc, 0)
	c.subscribed = make([]string, 5)
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	return c, nil
}

func releaseConn(c *connection) {
	c.id = ""
	connPool.Put(c)
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

const (
	// WriteWait is 1 second at the internal implementation,
	// same as here but this can be changed at the future*
	WriteWait = 1 * time.Second
)

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
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
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

// messageReceived checks the incoming message and fire the nativeMessage listeners or the event listeners (ws custom message)
func (c *connection) messageReceived(data []byte) error {

	if bytes.HasPrefix(data, c.server.config.EvtMessagePrefix) {
		//it's a custom ws message
		topic := c.server.messageSerializer.GetMsgTopic(data)
		customMessage, err := c.server.messageSerializer.Deserialize(topic, data)
		if err != nil {
			return err
		}
		switch topic.FirstFragment() {
		case message.SysTopic:
			if topic.Fragment(1) == "admin" && !strings.HasPrefix(c.request.RemoteAddr, "127.0.0.1") {
				log.Warn().Str("addr", c.request.RemoteAddr).Str("topic", topic.String()).Msg("receive invalid admin command")
				return nil
			}
			switch topic.String() {
			case message.TopicSubscribe.String():
				// TODO: 权限验证
				if s, is := customMessage.(string); is {
					c.Subscribe(s)
				}
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
				conn := c.server.GetConnection(customMessage.(string))
				if conn != nil {
					log.HandlerErrs(conn.echo(message.TopicAuth, "exit"), conn.Disconnect(nil))
				}
			default:
				log.Warn().Err(message.ErrUnformedMsg).Msg(string(data))
			}
		case message.InnerTopic:
			listeners, ok := c.onTopicListeners[topic.String()]
			if !ok || len(listeners) == 0 {
				return nil // if not listeners for this event exit from here
			}
			for i := range listeners {
				if fn, ok := listeners[i].(func()); ok { // its a simple func(){} callback
					fn()
				} else if fnString, ok := listeners[i].(func(string)); ok {

					if msgString, is := customMessage.(string); is {
						fnString(msgString)
					} else if msgInt, is := customMessage.(int); is {
						// here if server side waiting for string but client side sent an int, just convert this int to a string
						fnString(strconv.Itoa(msgInt))
					}

				} else if fnInt, ok := listeners[i].(func(int)); ok {
					fnInt(customMessage.(int))
				} else if fnBool, ok := listeners[i].(func(bool)); ok {
					fnBool(customMessage.(bool))
				} else if fnBytes, ok := listeners[i].(func([]byte)); ok {
					fnBytes(customMessage.([]byte))
				} else {
					listeners[i].(func(interface{}))(customMessage)
				}

			}
		default:
			// TODO 广播权限验证
			_ = c.server.Broadcast(c.server.topics.Match(topic.String()), data)
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

func (c *connection) EchoInner(innerTopic string, data interface{}) error {
	topic := message.NewTopic(innerTopic)
	if topic.FirstFragment() != "inner" {
		return message.ErrNotAllowedTopic
	}
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

func (c *connection) OnInner(innerTopic string, cb MessageFunc) {
	topic := message.NewTopic(innerTopic)
	if topic.FirstFragment() != "inner" {
		log.Warn().Err(message.ErrNotAllowedTopic).Msg(innerTopic)
		return
	}
	if c.onTopicListeners[topic.String()] == nil {
		c.onTopicListeners[topic.String()] = make([]MessageFunc, 0)
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
		c.fireDisconnect()
		c.server.CancelAll(c.id)
		c.server.connections.Delete(c.id)
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
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				return err
			}
		}
		releaseConn(c)
		return nil
	}
	return nil
}
