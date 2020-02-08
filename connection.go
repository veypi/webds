package webds

import (
	"bytes"
	"errors"
	"github.com/lightjiang/utils"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// UnderlineConnection is the underline connection, nothing to think about,
// it's used internally mostly but can be used for extreme cases with other libraries.
type UnderlineConnection interface {
	// SetWriteDeadline sets the write deadline on the underlying network
	// connection. After a write has timed out, the websocket state is corrupt and
	// all future writes will return an error. A zero value for t means writes will
	// not time out.
	SetWriteDeadline(t time.Time) error
	// SetReadDeadline sets the read deadline on the underlying network connection.
	// After a read has timed out, the websocket connection state is corrupt and
	// all future reads will return an error. A zero value for t means reads will
	// not time out.
	SetReadDeadline(t time.Time) error
	// SetReadLimit sets the maximum size for a message read from the peer. If a
	// message exceeds the limit, the connection sends a close frame to the peer
	// and returns ErrReadLimit to the application.
	SetReadLimit(limit int64)
	// SetPongHandler sets the handler for pong messages received from the peer.
	// The appData argument to h is the PONG frame application data. The default
	// pong handler does nothing.
	SetPongHandler(h func(appData string) error)
	// SetPingHandler sets the handler for ping messages received from the peer.
	// The appData argument to h is the PING frame application data. The default
	// ping handler sends a pong to the peer.
	SetPingHandler(h func(appData string) error)
	// WriteControl writes a control message with the given deadline. The allowed
	// message types are CloseMessage, PingMessage and PongMessage.
	WriteControl(messageType int, data []byte, deadline time.Time) error
	// WriteMessage is a helper method for getting a writer using NextWriter,
	// writing the message and closing the writer.
	WriteMessage(messageType int, data []byte) error
	// ReadMessage is a helper method for getting a reader using NextReader and
	// reading from that reader to a buffer.
	ReadMessage() (messageType int, p []byte, err error)
	// NextWriter returns a writer for the next message to send. The writer's Close
	// method flushes the complete message to the network.
	//
	// There can be at most one open writer on a connection. NextWriter closes the
	// previous writer if the application has not already done so.
	NextWriter(messageType int) (io.WriteCloser, error)
	// Close closes the underlying network connection without sending or waiting for a close frame.
	Close() error
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------Connection implementation-----------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

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
		// Err is not nil if the upgrader failed to upgrade http to websocket connection.
		Err() error

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
		OnPing(PingFunc)
		// OnPong  registers a callback which fires on pong message received
		OnPong(PongFunc)
		// FireOnError can be used to send a custom error message to the connection
		//
		// It does nothing more than firing the OnError listeners. It doesn't send anything to the client.
		FireOnError(err error)
		// To defines on which "topic" the server should send a message
		// returns an Publisher to send messages.
		Publisher(string) Publisher
		// On registers a callback to a particular topic which is fired when a message to this topic is received
		// just for topic with prefix '/inner'
		On(string, MessageFunc)

		// subscribe registers this connection to a topic, if it doesn't exist then it creates a new.
		// One topic can have one or more connections. One connection can subscribe many topics.
		// All connections subscribe a topic specified by their `ID` automatically.
		Subscribe(string)

		IsSubscribe(string) bool
		// Leave removes this connection entry from a room
		// Returns true if the connection has actually left from the particular room.
		CancelSubscribe(string)
		// Wait starts the pinger and the messages reader,
		// it's named as "Wait" because it should be called LAST,
		// after the "Subscribe" events IF server's `Upgrade` is used,
		// otherise you don't have to call it because the `Handler()` does it automatically.
		Wait()
		// Disconnect disconnects the client, close the underline websocket conn and removes it from the conn list
		// returns the error, if any, from the underline connection
		Disconnect() error
	}

	connection struct {
		err                   error
		underline             UnderlineConnection
		id                    string
		messageType           int
		disconnected          utils.SafeBool
		onDisconnectListeners []DisconnectFunc
		onErrorListeners      []ErrorFunc
		onPingListeners       []PingFunc
		onPongListeners       []PongFunc
		onTopicListeners      map[string][]MessageFunc
		subscribed            []string
		// mu for self object write/read
		selfMU  sync.RWMutex
		started utils.SafeBool

		server *Server
		// #119 , websocket writers are not protected by locks inside the gorilla's websocket code
		// so we must protect them otherwise we're getting concurrent connection error on multi writers in the same time.
		writerMu sync.Mutex
		// same exists for reader look here: https://godoc.org/github.com/gorilla/websocket#hdr-Control_Messages
		// but we only use one reader in one goroutine, so we are safe.
		// readerMu sync.Mutex
	}
)

var _ Connection = &connection{}

var connPool = sync.Pool{New: func() interface{} {
	return &connection{}
}}

func acquireConn(s *Server, underlineConnection UnderlineConnection, id string) *connection {
	c := connPool.Get().(*connection)
	c.underline = underlineConnection
	c.server = s
	c.id = id
	c.messageType = websocket.TextMessage
	if s.config.BinaryMessages {
		c.messageType = websocket.BinaryMessage
	}
	c.onDisconnectListeners = make([]DisconnectFunc, 0)
	c.onErrorListeners = make([]ErrorFunc, 0)
	c.onTopicListeners = make(map[string][]MessageFunc, 0)
	c.onPongListeners = make([]PongFunc, 0)
	c.onPingListeners = make([]PingFunc, 0)
	c.subscribed = make([]string, 5)
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	c.err = nil
	return c
}

func releaseConn(c *connection) {
	connPool.Put(c)
}

// Err is not nil if the upgrader failed to upgrade http to websocket connection.
func (c *connection) Err() error {
	return c.err
}

// Write writes a raw websocket message with a specific type to the client
// used by ping messages and any CloseMessage types.
func (c *connection) write(websocketMessageType int, data []byte) (int, error) {
	// for any-case the app tries to write from different goroutines,
	// we must protect them because they're reporting that as bug...
	c.writerMu.Lock()
	if writeTimeout := c.server.config.WriteTimeout; writeTimeout > 0 {
		// set the write deadline based on the configuration
		log.HandlerErrs(c.underline.SetWriteDeadline(time.Now().Add(writeTimeout)))
	}

	// .WriteMessage same as NextWriter and close (flush)
	err := c.underline.WriteMessage(websocketMessageType, data)
	c.writerMu.Unlock()
	if err != nil {
		// if failed then the connection is off, fire the disconnect
		log.HandlerErrs(c.Disconnect())
		return 0, err
	}
	return len(data), nil
}

// writeDefault is the same as write but the message type is the configured by c.messageType
// if BinaryMessages is enabled then it's raw []byte as you expected to work with protobufs
func (c *connection) Write(data []byte) (int, error) {
	return c.write(c.messageType, data)
}

const (
	// WriteWait is 1 second at the internal implementation,
	// same as here but this can be changed at the future*
	WriteWait = 1 * time.Second
)

func (c *connection) startPinger() {

	// this is the default internal handler, we just change the writeWait because of the actions we must do before
	// the server sends the ping-pong.

	pingHandler := func(message string) error {
		err := c.underline.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(WriteWait))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Temporary() {
			return nil
		}
		return err
	}

	c.underline.SetPingHandler(pingHandler)

	go func() {
		for {
			// using sleep avoids the ticker error that causes a memory leak
			time.Sleep(c.server.config.PingPeriod)
			if c.disconnected.IfTrue() {
				// verifies if already disconected
				break
			}
			//fire all OnPing methods
			c.fireOnPing()
			// try to ping the client, if failed then it disconnects
			_, err := c.write(websocket.PingMessage, []byte{})
			if err != nil {
				// must stop to exit the loop and finish the go routine
				break
			}
		}
	}()
}

func (c *connection) fireOnPing() {
	// fire the onPingListeners
	for i := range c.onPingListeners {
		c.onPingListeners[i]()
	}
}

func (c *connection) fireOnPong() {
	// fire the onPongListeners
	for i := range c.onPongListeners {
		c.onPongListeners[i]()
	}
}

func (c *connection) startReader() {
	conn := c.underline
	hasReadTimeout := c.server.config.ReadTimeout > 0

	conn.SetReadLimit(c.server.config.MaxMessageSize)
	conn.SetPongHandler(func(s string) error {
		if hasReadTimeout {
			log.HandlerErrs(conn.SetReadDeadline(time.Now().Add(c.server.config.ReadTimeout)))
		}

		c.fireOnPong()

		return nil
	})

	defer func() {
		log.HandlerErrs(c.Disconnect())
	}()

	for {
		if hasReadTimeout {
			// set the read deadline based on the configuration
			log.HandlerErrs(conn.SetReadDeadline(time.Now().Add(c.server.config.ReadTimeout)))
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			log.HandlerErrs(err)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
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

var UnformedMsg = errors.New("unformed Msg")

// messageReceived checks the incoming message and fire the nativeMessage listeners or the event listeners (ws custom message)
func (c *connection) messageReceived(data []byte) error {

	if bytes.HasPrefix(data, c.server.config.EvtMessagePrefix) {
		//it's a custom ws message
		topic := c.server.messageSerializer.GetMsgTopic(data)
		customMessage, err := c.server.messageSerializer.Deserialize(topic.String(), data)
		if customMessage == nil || err != nil {
			return UnformedMsg
		}
		switch topic.FirstFragment() {
		case "sys":
			switch topic.Since(1) {
			case "subscribe":
				// TODO: 权限验证
				if s, is := customMessage.(string); is {
					c.Subscribe(s)
				}
			case "cancel_subscribe":
				if s, is := customMessage.(string); is {
					c.CancelSubscribe(s)
				}
			case "cancel_all":
				c.server.CancelAll(c.id)

			}
		case "inner":
			listeners, ok := c.onTopicListeners[topic.Since(1)]
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
		return UnformedMsg
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

func (c *connection) OnPing(cb PingFunc) {
	c.onPingListeners = append(c.onPingListeners, cb)
}

func (c *connection) OnPong(cb PongFunc) {
	c.onPongListeners = append(c.onPongListeners, cb)
}

func (c *connection) FireOnError(err error) {
	for _, cb := range c.onErrorListeners {
		cb(err)
	}
}

func (c *connection) On(InnerTopic string, cb MessageFunc) {
	topic := message.Topic(InnerTopic)
	var s = topic.Since(0)
	if topic.FirstFragment() == "inner" {
		s = topic.Since(1)
	}
	if c.onTopicListeners[s] == nil {
		c.onTopicListeners[s] = make([]MessageFunc, 0)
	}
	c.onTopicListeners[s] = append(c.onTopicListeners[s], cb)
}

func (c *connection) Publisher(topic string) Publisher {
	return newPublisher(c, topic)
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

// Wait starts the pinger and the messages reader,
// it's named as "Wait" because it should be called LAST,
// after the "On" events IF server's `Upgrade` is used,
// otherise you don't have to call it because the `Handler()` does it automatically.
func (c *connection) Wait() {
	if c.started.SetTrue() {
		// start the ping
		c.startPinger()
		// start the messages reader
		c.startReader()
		return
	}
}

func (c *connection) Disconnect() error {
	if c.disconnected.SetTrue() {
		c.fireDisconnect()
		c.server.CancelAll(c.id)
		c.server.connections.Delete(c.id)
		err := c.underline.Close()
		if err != nil {
			return err
		}
		releaseConn(c)
		return nil
	}
	return nil
}
