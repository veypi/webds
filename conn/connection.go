package conn

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/veypi/utils"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/cfg"
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

var ErrID = errors.New("id is not exist")

var connPool = sync.Pool{
	New: func() interface{} {
		return new(conn)
	},
}

var nodeStatusMsg = func() func(id string, status string) []byte {
	m := &message.Message{}
	m.Target = message.TopicNodeStatus.String()
	m.Type = message.Message_String
	return func(id string, status string) []byte {
		m.Data = []byte(id + ":" + status)
		res, err := proto.Marshal(m)
		if err != nil {
			log.Warn().Msg(err.Error())
			return nil
		}
		return res
	}
}()

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
	c.meta = nil
	connPool.Put(c)
}

func getConn() *conn {
	return connPool.Get().(*conn)
	//return new(conn)
}

// 接收请求建立连接
func NewPassiveConn(w http.ResponseWriter, r *http.Request, cfg *cfg.Config) (core.Connection, error) {
	cfg.Validate()
	c := getConn()
	c.cfg = cfg
	c.passive = true
	c.ctx, c.stop = context.WithCancel(cfg.Ctx())
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
	c.onDisconnectListeners = message.NewSubscriberList()
	c.onConnectListeners = message.NewSubscriberList()
	c.onErrorListeners = message.NewSubscriberList()
	c.onTopicListeners = make(map[string]*message.SubscriberList)
	c.id = cfg.IDGenerator(c)
	if c.id == "" {
		c.echo(message.TopicAuth, ErrID.Error())
		time.Sleep(time.Millisecond)
		c.Close()
		return nil, ErrID
	}
	if cfg.Webds() != nil && !cfg.Webds().AddConnection(c) {
		c.echo(message.TopicAuth, ErrDuplicatedConn.Error())
		time.Sleep(time.Millisecond)
		c.Close()
		return nil, fmt.Errorf("%s->%s: %w", c.id, cfg.Webds().ID(), ErrDuplicatedConn)
	}
	c.webds = cfg.Webds()
	c.msgType = websocket.MessageBinary
	c.echo(message.TopicAuth, "pass")
	c.webds.Broadcast(message.TopicNodeStatus.String(), nodeStatusMsg(c.id, "1"), c.id)
	return c, nil
}

// 发起请求建立连接
func NewActiveConn(id, host string, port uint, path string, cfg *cfg.Config, headers ...string) (core.Connection, error) {
	cfg.Validate()
	var err error
	c := getConn()
	c.cfg = cfg
	c.passive = false
	c.id = id
	c.host = host
	c.port = port
	c.path = path
	c.ctx, c.stop = context.WithCancel(cfg.Ctx())
	header := http.Header{"id": []string{c.id}}
	for i := 0; i < len(headers)-1; i += 2 {
		header[headers[i]] = []string{headers[i+1]}
	}
	c.line, _, err = websocket.Dial(c.ctx, c.TargetUrl(), &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		releaseConn(c)
		return nil, err
	}
	c.started.ForceSetFalse()
	c.disconnected.ForceSetFalse()
	c.msgType = websocket.MessageBinary
	if cfg.Webds() != nil && !cfg.Webds().AddConnection(c) {
		c.Close()
		return nil, fmt.Errorf("%s->%s: %w", id, cfg.Webds().ID(), ErrDuplicatedConn)
	}
	c.webds = cfg.Webds()
	c.onDisconnectListeners = message.NewSubscriberList()
	c.onConnectListeners = message.NewSubscriberList()
	c.onErrorListeners = message.NewSubscriberList()
	c.onTopicListeners = make(map[string]*message.SubscriberList)
	return c, nil
}

var _ core.Connection = &conn{}

type conn struct {
	ctx       context.Context
	line      *websocket.Conn
	id        string
	passive   bool
	clusterID string
	cfg       *cfg.Config

	request *http.Request
	// 被动方式下为本身信息 主动模式下为对方信息
	targetID  string
	host      string
	port      uint
	path      string
	targetUrl string
	msgType   websocket.MessageType

	started      utils.SafeBool
	disconnected utils.SafeBool
	stop         context.CancelFunc
	stopWait     *sync.WaitGroup
	msgChan      chan *message.Message
	selfMU       sync.RWMutex

	onDisconnectListeners *message.SubscriberList
	onConnectListeners    *message.SubscriberList
	onErrorListeners      *message.SubscriberList
	onTopicListeners      map[string]*message.SubscriberList
	webds                 core.Webds
	meta                  *sync.Map
}

func (c *conn) String() string {
	if c.passive {
		return c.targetID + " <-- " + c.id + "(" + c.TargetUrl() + ")"
	}
	return c.id + " --> " + c.targetID + "(" + c.TargetUrl() + ")"
}

func (c *conn) Ctx() context.Context {
	return c.ctx

}

func (c *conn) Request() *http.Request {
	return c.request
}

func (c *conn) ClusterID() string {
	return c.clusterID
}
func (c *conn) SetClusterID(s string) {
	c.clusterID = s
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

func (c *conn) OnDelta(t time.Ticker, cb func()) {
	if c.started.IfTrue() && !c.disconnected.IfTrue() {
		log.Warn().Msg("conn not started or has been closed.")
	}
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Error().Err(nil).Msgf("conn(%s)'s period func occurred error %v", c.id, e)
			}
		}()
		select {
		case <-c.Ctx().Done():
			return
		case <-t.C:
			cb()
		}
	}()
	return
}

func (c *conn) OnConnect(cb core.ConnectFunc) *message.Subscriber {
	if c.started.IfTrue() {
		cb()
		return nil
	}
	return c.onConnectListeners.Add(cb)
}

func (c *conn) OnDisconnect(cb core.DisconnectFunc) *message.Subscriber {
	if c.disconnected.IfTrue() {
		// 如果已经断开连接则立即触发
		cb()
		return nil
	}
	return c.onDisconnectListeners.Add(cb)
}

func (c *conn) OnError(errorFunc core.ErrorFunc) *message.Subscriber {
	return c.onErrorListeners.Add(errorFunc)
}

func (c *conn) Publisher(s string) func(interface{}) {
	t := message.NewTopic(s)
	if !message.IsPublicTopic(t) {
		panic(message.ErrNotAllowedTopic)
	}
	s = t.String()
	if c.Passive() {
		return func(i interface{}) {
			m, err := message.Encode(t, i)
			if err != nil {
				log.HandlerErrs(err)
				return
			}
			c.webds.Broadcast(s, m, c.id)
		}

	}
	return func(data interface{}) {
		c.echo(t, data)
	}
}

func (c *conn) Echo(t message.Topic, data interface{}) {
	c.echo(t, data)
}

func (c *conn) echo(t message.Topic, data interface{}) {
	m, err := message.Encode(t, data)
	if err != nil {
		log.HandlerErrs(err)
		return
	}
	_, err = c.Write(m)
	if err != nil && ignoreErr(err) != nil {
		log.Trace().Err(err).Msg("write error")
	}
}

func (c *conn) Subscribe(t message.Topic, m message.Func) *message.Subscriber {
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
	if c.Alive() && message.IsPublicTopic(t) && !c.Passive() {
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
	//if c.disconnected.IfTrue() {
	//	return 0, errors.New("connection has closed")
	//}
	defer func() {
		if e := recover(); e != nil {
			log.Warn().Msgf("write error %v", e)
		}
	}()
	err := c.line.Write(c.ctx, websocketMessageType, p)
	return len(p), err
}

func (c *conn) startPing() {
	defer func() {
		c.stopWait.Done()
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("ping error: %v", e)
		}
	}()
	var err error
	line := c.line
	clock := time.Tick(time.Minute)
	for {
		// using sleep avoids the ticker error that causes a memory leak
		select {
		case <-clock:
		case <-c.Ctx().Done():
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

func (c *conn) startReader() (err error) {
	c.line.SetReadLimit(c.cfg.ReadBufferSize())
	defer func() {
		c.stopWait.Done()
		log.HandlerErrs(c.Close(), err)
	}()
	line := c.line
	msgChan := c.msgChan
	for {
		m := message.New()
		_, buf, err := line.Read(c.ctx)
		//err = wspb.Read(c.ctx, line, m)
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
		err = proto.Unmarshal(buf, m)
		if err != nil {
			log.Warn().Msg(err.Error())
			return err
		}
		if !c.disconnected.IfTrue() {
			msgChan <- m
		}
	}
}

func (c *conn) msgReader() {
	defer func() {
		c.stopWait.Done()
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("msg error: %v", e)
		}
		log.HandlerErrs(c.Close())
	}()
	msgChan := c.msgChan
	var err error
	for {
		select {
		case m := <-msgChan:
			if m == nil {
				return
			}
			err = c.onMsg(m)
			if err != nil {
				log.Warn().Msg(err.Error())
			}
			m.Release()
		case <-c.Ctx().Done():
			return

		}
	}
}

func (c *conn) Wait() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("connection closed: %v", e)
			log.Error().Err(err).Msg("")
		}
	}()
	if c.started.SetTrue() {
		c.stopWait = &sync.WaitGroup{}
		c.stopWait.Add(3)
		c.msgChan = make(chan *message.Message, 100)
		go c.startPing()
		// start the messages reader
		go c.msgReader()
		return c.startReader()

	}
	log.Warn().Msg("connection has been started: " + utils.CallPath(1))
	return nil
}

func (c *conn) Close() error {
	if c.disconnected.SetTrue() {
		//log.Trace().Msgf("%s closed, called from %s %v", c.String(), utils.CallPath(1), c.webds != nil)
		err := c.line.Close(websocket.StatusNormalClosure, "")
		c.fireDisconnect()
		c.stop()
		if c.started.IfTrue() {
			if c.msgChan != nil {
				close(c.msgChan)
				c.msgChan = nil
			}
			c.stopWait.Wait()
			c.stopWait = nil
		}
		if c.webds != nil {
			c.webds.DelConnection(c.id)
		}
		releaseConn(c)
		if err != nil {
			err = errors.Unwrap(err)
			return ignoreErr(err)
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
	c.onConnectListeners.Range(func(s *message.Subscriber) {
		s.Do(nil)
	})
}

func (c *conn) fireDisconnect() {
	if c.Passive() && c.webds != nil {
		c.webds.Broadcast(message.TopicNodeStatus.String(), nodeStatusMsg(c.id, "0"), c.id)
	}
	if c.onDisconnectListeners == nil {
		return
	}
	c.onDisconnectListeners.Range(func(s *message.Subscriber) {
		s.Do(nil)
	})
}

func (c *conn) FireOnError(err error) {
	if c.onErrorListeners == nil {
		return
	}
	c.onErrorListeners.Range(func(s *message.Subscriber) {
		s.Do(err)
	})
}

func (c *conn) onMsg(m *message.Message) error {
	topic := message.NewTopic(m.Target)
	if message.IsSysTopic(topic) {
		log.HandlerErrs(c.onSysMsg(topic, m))
	} else if message.IsPublicTopic(topic) && c.webds != nil {
		m.Source = m.Source + "/" + c.ID()
		buf, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		if c.webds != nil && c.webds.Cluster() != nil {
			c.webds.Cluster().RangeCluster(func(nc core.Connection) bool {
				if nc.ID() != c.id {
					nc.Write(buf)
				}
				return true
			})
		}
		c.webds.Broadcast(topic.String(), buf, c.id)
	}
	ts := topic.String()
	for t, listeners := range c.onTopicListeners {
		if listeners.Len() == 0 {
			continue
		}
		if strings.HasPrefix(ts, t) {
			listeners.Range(func(s *message.Subscriber) {
				s.Do(m)
			})
		}
	}
	if c.cfg.EnableCluster && c.webds != nil {
		c.webds.FireMsg(m)
	}
	return nil
}

func (c *conn) onSysMsg(t message.Topic, m *message.Message) error {
	if strings.HasSuffix(t.String(), "admin") {
		// TODO 敏感操作鉴权
	}
	switch t.Fragment(1) {
	case "base":
		return c.onBaseMsg(t, m)
	case "topic":
		if c.webds != nil {
			return c.onTopicMsg(t, m)
		}
	case "node":
		if c.webds != nil {
			return c.onTopicMsg(t, m)
		}
	case "cluster":
		if c.webds != nil && c.webds.Cluster() != nil {
			c.webds.Cluster().Receive(c, t, m.Body())
			return nil
		}
	}
	return nil
}

func (c *conn) onTopicMsg(t message.Topic, m *message.Message) error {
	switch t.String() {
	case message.TopicSubscribe.String():
		c.webds.Subscribe(m.Body().(string), c.id)
	case message.TopicSubscribeAll.String():
		c.webds.Subscribe("", c.id)
	case message.TopicCancel.String():
		c.webds.CancelSubscribe(m.Body().(string), c.id)
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
			c.webds.Subscribe(message.TopicNodeStatus.String(), c.id)
		}
	case message.TopicStopNode.String():
		// 仅中断连接
		if tempC := c.webds.GetConnection(m.Body().(string)); tempC != nil {
			tempC.Echo(message.TopicStopNode, "exit")
			log.HandlerErrs(tempC.Close())
		}

	}
	return nil
}

func (c *conn) onBaseMsg(t message.Topic, m *message.Message) error {
	switch t.String() {
	case message.TopicSysLog.String():
		log.Warn().Msgf("%v", m.Body())
	case message.TopicAuth.String():
		// TODO auth check
		if m.Body().(string) == "pass" {
			c.fireConnect()
			if c.Passive() {
				c.echo(message.TopicAuth, "pass")
			}
		} else {
			log.Warn().Interface("msg", m.Body()).Msg("auth failed")
		}
	}
	return nil
}

func (c *conn) Set(key string, v interface{}) {
	if c.meta == nil {
		c.meta = new(sync.Map)
	}
	c.meta.Store(key, v)
}
func (c *conn) Get(key string) interface{} {
	if c.meta != nil {
		v, _ := c.meta.Load(key)
		return v
	}
	return nil
}

func ignoreErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err.Error() == "already wrote close" {
		return nil
	} else if strings.HasSuffix(err.Error(), "context canceled") {
		return nil
	}
	if websocket.CloseStatus(err) == websocket.StatusGoingAway {
		return nil
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
		return nil
	}
	return err
}
