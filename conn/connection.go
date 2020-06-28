package conn

import (
	"bytes"
	"context"
	"errors"
	"github.com/veypi/utils"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/core"
	"github.com/veypi/webds/message"
	"io"
	"net/http"
	"nhooyr.io/websocket"
	"strings"
	"sync"
	"time"
)

var ErrDuplicatedConn = errors.New("duplicated conn")

var connPool = sync.Pool{
	New: func() interface{} {
		return new(conn)
	},
}

func releaseConn(c *conn) {
	c.id = ""
	c.line = nil
	c.webds = nil
	c.targetUrl = ""
	c.targetID = ""
	c.onDisconnectListeners = nil
	c.onConnectListeners = nil
	c.onErrorListeners = nil
	c.onTopicListeners = nil
	connPool.Put(c)
}

func getConn() *conn {
	return connPool.Get().(*conn)
}

// 接收请求建立连接
func NewPassiveConn(id string, w http.ResponseWriter, r *http.Request, cfg core.ConnCfg) (core.Connection, error) {
	cfg.Validate()
	c := getConn()
	c.level = -1
	c.cfg = cfg
	c.passive = true
	c.id = id
	c.ctx = cfg.Ctx()
	c.msgSerializer = cfg.MsgSerializer()
	var err error
	c.line, err = websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       nil,
		InsecureSkipVerify: true,
	})
	if err != nil {
		releaseConn(c)
		return nil, err
	}
	c.host, c.port, c.path = core.DecodeUrl(r.Host)
	c.path = r.URL.Path
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	c.request = r
	c.webds = nil
	if cfg.Webds() != nil && !cfg.Webds().AddConnection(c) {
		c.echo(message.TopicAuth, ErrDuplicatedConn.Error())
		c.Close()
		return nil, ErrDuplicatedConn
	}
	c.webds = cfg.Webds()
	if cfg.BinaryMessages() {
		c.msgType = websocket.MessageBinary
	} else {
		c.msgType = websocket.MessageText
	}
	c.onDisconnectListeners = message.NewSubscriberList()
	c.onConnectListeners = message.NewSubscriberList()
	c.onErrorListeners = message.NewSubscriberList()
	c.onTopicListeners = make(map[string]message.SubscriberList)
	c.echo(message.TopicAuth, "pass")
	return c, nil
}

// 发起请求建立连接
func NewActiveConn(id, host string, port uint, path string, cfg core.ConnCfg) (core.Connection, error) {
	cfg.Validate()
	var err error
	c := getConn()
	c.cfg = cfg
	c.level = 1
	c.passive = false
	c.id = id
	c.ctx = cfg.Ctx()
	c.host = host
	c.port = port
	c.path = path
	c.msgSerializer = cfg.MsgSerializer()
	c.line, _, err = websocket.Dial(c.ctx, c.TargetUrl(), &websocket.DialOptions{
		HTTPHeader: http.Header{"id": []string{c.id}},
	})
	if err != nil {
		releaseConn(c)
		return nil, err
	}
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	if cfg.BinaryMessages() {
		c.msgType = websocket.MessageBinary
	} else {
		c.msgType = websocket.MessageText
	}
	c.webds = nil
	if cfg.Webds() != nil && !cfg.Webds().AddConnection(c) {
		c.Close()
		return nil, ErrDuplicatedConn
	}
	c.webds = cfg.Webds()
	c.onDisconnectListeners = message.NewSubscriberList()
	c.onConnectListeners = message.NewSubscriberList()
	c.onErrorListeners = message.NewSubscriberList()
	c.onTopicListeners = make(map[string]message.SubscriberList)
	return c, nil
}

var _ core.Connection = &conn{}

type conn struct {
	ctx     context.Context
	line    *websocket.Conn
	id      string
	passive bool
	cfg     core.ConnCfg

	request *http.Request
	// 被动方式下为本身信息 主动模式下为对方信息
	targetID  string
	host      string
	port      uint
	path      string
	targetUrl string
	// <0: 下级节点， 只能对方主动发起请求， passive 只能为true
	// 0: 平级节点， 双方可以互相访问， passive true/false
	// >0: 上级节点， 只能向对方发起请求， passive 只能为false
	level   int
	msgType websocket.MessageType

	started      utils.SafeBool
	disconnected utils.SafeBool
	stop         chan bool
	msgChan      chan []byte
	selfMU       sync.RWMutex

	onDisconnectListeners message.SubscriberList
	onConnectListeners    message.SubscriberList
	onErrorListeners      message.SubscriberList
	onTopicListeners      map[string]message.SubscriberList
	webds                 core.Webds
	msgSerializer         *message.Serializer
}

func (c *conn) String() string {
	return c.id + " --> " + c.targetID + "(" + c.TargetUrl() + ")"
}

func (c *conn) TargetUrl() string {
	if c.targetUrl == "" {
		c.targetUrl = core.EncodeUrl(c.host, c.port, c.path)
	}
	return c.targetUrl
}

func (c *conn) TargetID() string {
	return c.targetID
}

func (c *conn) SetTargetID(s string) {
	c.targetID = s
}

func (c *conn) SetTargetHost(s string) {
	c.host = s
}

func (c *conn) SetTargetPort(s uint) {
	c.port = s
}

func (c *conn) SetTargetPath(s string) {
	c.path = s
}

func (c *conn) ID() string {
	return c.id
}

func (c *conn) Passive() bool {
	return c.passive
}

func (c *conn) Level() int {
	return c.level
}

func (c *conn) SetLevel(i int) {
	c.level = i
}

func (c *conn) OnDelta(t time.Ticker, cb func()) {
	if c.started.IfTrue() && !c.disconnected.IfTrue() {
		log.Warn().Msg("conn not started or has been closed.")
	}
	stop := c.stop
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Error().Err(nil).Msgf("conn(%s)'s period func occurred error %v", c.id, e)
			}
		}()
		select {
		case <-stop:
			return
		case <-t.C:
			cb()
		}
	}()
	return
}

func (c *conn) OnConnect(cb core.ConnectFunc) message.Subscriber {
	if c.started.IfTrue() {
		cb()
		return nil
	}
	return c.onConnectListeners.Add(cb)
}

func (c *conn) OnDisconnect(cb core.DisconnectFunc) message.Subscriber {
	if c.disconnected.IfTrue() {
		// 如果已经断开连接则立即触发
		cb()
		return nil
	}
	return c.onDisconnectListeners.Add(cb)
}

func (c *conn) OnError(errorFunc core.ErrorFunc) message.Subscriber {
	return c.onErrorListeners.Add(errorFunc)
}

func (c *conn) Publisher(s string) func(interface{}) {
	t := message.NewTopic(s)
	if !message.IsPublicTopic(t) {
		panic(message.ErrNotAllowedTopic)
	}
	return func(data interface{}) {
		c.echo(t, data)
	}
}

func (c *conn) Echo(t message.Topic, data interface{}) {
	if message.IsPublicTopic(t) {
		log.HandlerErrs(message.ErrNotAllowedTopic)
		return
	}
	c.echo(t, data)
}

func (c *conn) echo(t message.Topic, data interface{}) {
	m, err := c.msgSerializer.Serialize(t, data)
	if err != nil {
		log.HandlerErrs(err)
		return
	}
	_, err = c.Write(m)
	if err != nil {
		log.HandlerErrs(err)
		c.Close()
	}
}

func (c *conn) Subscribe(t message.Topic, m message.Func) message.Subscriber {
	s := t.String()
	if m == nil {
		return nil
	}
	if c.onTopicListeners[s] == nil {
		c.onTopicListeners[s] = message.NewSubscriberList()
		c.subscribe(t)
	}
	return c.onTopicListeners[s].Add(m)
}
func (c *conn) subscribe(t message.Topic) {
	if c.Alive() && message.IsPublicTopic(t) {
		c.echo(message.TopicSubscribe, t.String())
	}
}

func (c *conn) Write(p []byte) (n int, err error) {
	// TODO 是否要通过 chan 写, 用以控制状态
	return c.write(c.msgType, p)
}
func (c *conn) write(websocketMessageType websocket.MessageType, p []byte) (int, error) {
	// for any-case the app tries to write from different goroutines,
	//c.selfMU.Lock()
	//defer c.selfMU.Unlock()
	// TODO: 是否有必要加锁
	err := c.line.Write(c.ctx, websocketMessageType, p)
	return len(p), err
}

func (c *conn) startPing() {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("ping error: %v", e)
		}
	}()
	var err error
	line := c.line
	clock := time.Tick(time.Minute)
	stop := c.stop
	for {
		// using sleep avoids the ticker error that causes a memory leak
		select {
		case <-clock:
		case <-stop:
			return
		}
		// try to ping the client, if failed then it disconnects
		err = line.Ping(c.ctx)
		if err != nil {
			log.HandlerErrs(err)
			// must stop to exit the loop and finish the go routine
			break
		}
	}
}

func (c *conn) startReader() error {
	c.line.SetReadLimit(c.cfg.ReadBufferSize())
	defer func() {
		log.HandlerErrs(c.Close())
	}()
	line := c.line
	msgChan := c.msgChan
	var buf []byte
	var err error
	for {
		_, buf, err = line.Read(c.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			switch websocket.CloseStatus(err) {
			case websocket.StatusNormalClosure:
			case websocket.StatusGoingAway:
			default:
				c.FireOnError(err)
				return err
			}
			return nil
		}
		msgChan <- buf
		buf = nil
		err = nil
	}
}

func (c *conn) msgReader() {
	defer func() {
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("ping error: %v", e)
		}
		log.HandlerErrs(c.Close())
	}()
	msgChan := c.msgChan
	var err error
	for data := range msgChan {
		err = c.onMsg(data)
		if err != nil {
			log.Warn().Msg(err.Error())
		}
	}
}

func (c *conn) Wait() error {
	if c.started.SetTrue() {
		c.stop = make(chan bool)
		c.msgChan = make(chan []byte, 100)
		go c.startPing()
		// start the messages reader
		go c.msgReader()
		return c.startReader()

	}
	log.Warn().Msg("connection has been started")
	return nil
}

func (c *conn) Close() error {
	if c.disconnected.SetTrue() {
		log.Debug().Msgf("%s (%v) closed, called from %s", c.String(), c.Passive(), utils.CallPath(1))
		if c.webds != nil {
			c.webds.DelConnection(c.id)
		}
		if c.started.IfTrue() {
			close(c.stop)
			close(c.msgChan)
			c.stop = nil
			c.msgChan = nil
		}
		err := c.line.Close(websocket.StatusNormalClosure, "")
		c.fireDisconnect()
		releaseConn(c)
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
		return nil
	}
	return nil
}

func (c *conn) Type() int {
	panic("implement me")
}

func (c *conn) Alive() bool {
	return c != nil && c.started.IfTrue() && !c.disconnected.IfTrue()
}

func (c *conn) fireConnect() {
	for v := range c.onTopicListeners {
		c.subscribe(message.NewTopic(v))
	}
	c.onConnectListeners.Range(func(s message.Subscriber) {
		s.Do(nil)
	})
}

func (c *conn) fireDisconnect() {
	if c.onDisconnectListeners == nil {
		return
	}
	c.onDisconnectListeners.Range(func(s message.Subscriber) {
		s.Do(nil)
	})
}

func (c *conn) FireOnError(err error) {
	if c.onErrorListeners == nil {
		return
	}
	c.onErrorListeners.Range(func(s message.Subscriber) {
		s.Do(err)
	})
}

func (c *conn) onMsg(data []byte) error {
	if !bytes.HasPrefix(data, c.msgSerializer.Prefix()) {
		return message.ErrUnformedMsg
	}
	topic := c.msgSerializer.GetMsgTopic(data)
	if message.IsSysTopic(topic) {
		log.HandlerErrs(c.onSysMsg(topic, data))
	} else if message.IsPublicTopic(topic) && c.webds != nil {
		if c.webds != nil && c.webds.Cluster() != nil {
			c.webds.Cluster().RangeConn(func(nc core.Connection) bool {
				if nc.ID() != c.id {
					nc.Write(data)
				}
				return true
			})
		}
		c.webds.Broadcast(topic.String(), data, c.id)
	}
	listeners, ok := c.onTopicListeners[topic.String()]
	if !ok || listeners.Len() == 0 {
		return nil
	}
	customMsg, err := c.msgSerializer.Deserialize(data)
	if err != nil {
		return err
	}
	listeners.Range(func(s message.Subscriber) {
		s.Do(customMsg)
	})
	return nil
}

func (c *conn) onSysMsg(t message.Topic, data []byte) error {
	if strings.HasSuffix(t.String(), "admin") {
		// TODO 敏感操作鉴权
	}
	switch t.Fragment(1) {
	case "base":
		return c.onBaseMsg(t, data)
	case "topic":
		if c.webds != nil {
			return c.onTopicMsg(t, data)
		}
	case "cluster":
		if c.webds != nil && c.webds.Cluster() != nil {
			customMessage, err := c.msgSerializer.Deserialize(data)
			if err != nil {
				return err
			}
			c.webds.Cluster().Receive(c, t, customMessage.(string))
			return nil
		}
	}
	return nil
}

func (c *conn) onTopicMsg(t message.Topic, data []byte) error {
	customMessage, err := c.msgSerializer.Deserialize(data)
	if err != nil {
		return err
	}
	switch t.String() {
	case message.TopicSubscribe.String():
		c.webds.Subscribe(customMessage.(string), c.id)
	case message.TopicSubscribeAll.String():
		c.webds.Subscribe("", c.id)
	case message.TopicCancel.String():
		c.webds.CancelSubscribe(customMessage.(string), c.id)
	case message.TopicCancelAll.String():
		c.webds.CancelAll(c.id)
	case message.TopicGetAllTopics.String():
		if c.Passive() {
			c.echo(message.TopicGetAllTopics, c.webds.Topics().String())
		}
	case message.TopicGetAllNodes.String():
		if c.Passive() {
			res := ""
			c.webds.Range(func(id string, tempC core.Connection) bool {
				if !tempC.Passive() {
					res += "->" + tempC.TargetID() + "\n"
				} else {
					res += id + "\n"
				}
				return true
			})
			c.echo(message.TopicGetAllNodes, res)
		}
	case message.TopicStopNode.String():
		// 仅中断连接
		if tempC := c.webds.GetConnection(customMessage.(string)); tempC != nil {
			tempC.Echo(message.TopicStopNode, "exit")
			log.HandlerErrs(tempC.Close())
		}

	}
	return nil
}

func (c *conn) onBaseMsg(t message.Topic, data []byte) error {
	customMessage, err := c.msgSerializer.Deserialize(data)
	if err != nil {
		return err
	}
	switch t.String() {
	case message.TopicSysLog.String():
		log.Warn().Msgf("%v", customMessage)
	case message.TopicAuth.String():
		// TODO auth check
		if s, ok := customMessage.(string); ok && s == "pass" {
			c.fireConnect()
			if c.Passive() {
				c.echo(message.TopicAuth, "pass")
			}
		} else {
			log.Warn().Interface("msg", customMessage).Msg("auth failed")
		}
	}
	return nil
}
