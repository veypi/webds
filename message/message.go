package message

import (
	"bytes"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/json-iterator/go"
	"github.com/veypi/utils/log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type (
	// A callback which should receives one parameter of type string, int, bool or any valid JSON/Go struct
	Func        interface{}
	FuncInt     = func(int)
	FuncString  = func(string)
	FuncError   = func(error)
	FuncBlank   = func()
	FuncBytes   = func([]byte)
	FuncBool    = func(bool)
	FuncDefault = func(interface{})
	RawFunc     = func(*Message)
)

var json = jsoniter.ConfigFastest

var (
	PublicTopic = NewTopic("")
	// 用户保留主题 不会进行广播
	InnerTopic = NewTopic("inner")
	// 系统保留主题 不会触发任何注册的回调函数
	SysTopic = NewTopic("sys")
)

var (
	TopicBase = SysTopic.Child("base")
	// 日志
	TopicSysLog = TopicBase.Child("log")
	// 连接权限验证
	TopicAuth = TopicBase.Child("auth")
)

var (
	TopicTopic = SysTopic.Child("topic")
	// 订阅指令
	TopicSubscribe = TopicTopic.Child("subscribe")
	// 取消订阅指令
	TopicCancel = TopicTopic.Child("cancel")
	// 取消所有订阅指令
	TopicCancelAll = TopicTopic.Child("cancel_all")
	// 敏感操作 需要通过判定
	TopicSubscribeAll = TopicTopic.Child("subscribe_all/admin")
	TopicGetAllTopics = TopicTopic.Child("get_all_topics/admin")
)

var (
	TopicNode        = SysTopic.Child("node")
	TopicGetAllNodes = TopicNode.Child("get_all_nodes/admin")
	TopicStopNode    = TopicNode.Child("stop_node/admin")
	TopicNodeStatus  = TopicNode.Child("status/admin")
)

var (
	TopicCluster = SysTopic.Child("cluster")
	// 三次同步过程建立节点连接
	// 交换id
	TopicClusterID = TopicCluster.Child("id")
	// 通知本身级别
	TopicClusterLevel = TopicCluster.Child("level")
	// 交换cluster节点信息
	TopicClusterInfo = TopicCluster.Child("info")
	// 是否跳转有子节点决定
	TopicClusterRedirect = TopicCluster.Child("redirect")
)

var (
	ErrNotAllowedTopic = errors.New("this topic is not allowed to subscribe or publish")
	ErrUnformedMsg     = errors.New("unformed Msg")
)

func IsPublicTopic(t Topic) bool {
	return !IsInnerTopic(t) && !IsSysTopic(t)
}

func IsInnerTopic(t Topic) bool {
	return t.IsChildOf(InnerTopic)
}

func IsSysTopic(t Topic) bool {
	return t.IsChildOf(SysTopic)
}

func NewTopic(t string) Topic {
	return topic("/" + strings.Trim(t, "/"))
}

type Topic interface {
	String() string
	Bytes() []byte
	FirstFragment() string
	Fragment(int) string
	Since(int) string
	Child(string) Topic
	IsChildOf(p Topic) bool
	Len() int
}

type topic []byte

func (t topic) String() string {
	return *(*string)(unsafe.Pointer(&t))
}

func (t topic) Bytes() []byte {
	return t
}

func (t topic) Len() int {
	return len(t)
}

func (t topic) FirstFragment() string {
	return t.Fragment(0)
}

func (t topic) Fragment(count int) string {
	var res string
	var tempCount = -1
	for _, i := range t.String() {
		if i == '/' {
			if tempCount == count {
				break
			}
			tempCount++
		} else if tempCount == count {
			res += string(i)
		}
	}
	return res
}

func (t topic) Since(count int) string {
	var index int
	var tempCount = -1
	for i, v := range t.String() {
		if v == '/' {
			if tempCount == count {
				break
			}
			tempCount++
			index = i
		}
	}
	return t.String()[index:]
}

func (t topic) IsChildOf(p Topic) bool {
	return bytes.HasPrefix(t, append(p.Bytes(), '/'))
}

func (t topic) Child(s string) Topic {
	if s == "" || s == "/" {
		panic("invalid topic")
	}
	if t.String()[t.Len()-1] == '/' {
		if s[0] == '/' {
			s = s[1:]
		}
		return NewTopic(t.String() + s)
	} else {
		if s[0] != '/' {
			s = "/" + s
		}
		return NewTopic(t.String() + s)
	}
}

var InvalidMessage = errors.New("invalid message")
var InvalidTopic = errors.New("invalid topic")
var msgPool = sync.Pool{New: func() interface{} {
	return &Message{}
}}

func New() *Message {
	return msgPool.Get().(*Message)
}

func (x *Message) Body() interface{} {
	switch x.Type {
	case Message_Bytes:
		return x.Data
	case Message_Int:
		d, err := strconv.Atoi(string(x.Data))
		if err != nil {
			log.Warn().Msg(err.Error())
		}
		return d
	case Message_String:
		return string(x.Data)
	case Message_Bool:
		return x.Data[0] == '1'
	default:
		return x.Data
	}
}

func (x *Message) Release() {
	x.Reset()
	msgPool.Put(x)
}

func Decode(b []byte) (*Message, error) {
	m := New()
	err := proto.Unmarshal(b, m)
	return m, err
}

var msgCounter uint64

func Encode(t Topic, data interface{}) ([]byte, error) {
	m := New()
	m.Source = "/s"
	m.UnixTime = time.Now().Unix()
	m.Target = t.String()
	m.Tag = atomic.AddUint64(&msgCounter, 1)
	switch d := data.(type) {
	case []byte:
		m.Type = Message_Bytes
		m.Data = d
	case string:
		m.Type = Message_String
		m.Data = []byte(d)
	case int:
		m.Type = Message_Int
		m.Data = []byte(strconv.Itoa(d))
	case bool:
		m.Type = Message_Bool
		if d {
			m.Data = []byte{'1'}
		} else {
			m.Data = []byte{'0'}
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		m.Type = Message_JSON
		m.Data = b
	}
	return proto.Marshal(m)
}
