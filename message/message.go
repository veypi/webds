package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/json-iterator/go"
	"github.com/lightjiang/utils/log"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

type (
	// A callback which should receives one parameter of type string, int, bool or any valid JSON/Go struct
	Func         interface{}
	FuncInt      = func(int)
	FuncString   = func(string)
	FuncBlank    = func()
	FuncBytes    = func([]byte)
	FuncBool     = func(bool)
	FuncDefault  = func(interface{})
	FuncTransfer = func([]byte, string, string)
)

var json = jsoniter.ConfigFastest

const (
	PublicTopic = ""
	// 用户保留主题 不会进行广播
	InnerTopic = "inner"
	// 系统保留主题 不会触发任何注册的回调函数
	SysTopic = "sys"
)

var (
	// 订阅指令
	TopicSubscribe = NewTopic("/sys/subscribe")
	// 取消订阅指令
	TopicCancel = NewTopic("/sys/cancel")
	// 取消所有订阅指令
	TopicCancelAll = NewTopic("/sys/cancel_all")
	// 敏感操作 仅来自127.0.0.1的节点通过判定
	TopicSubscribeAll = NewTopic("/sys/admin/subscribe_all")
	TopicGetAllTopics = NewTopic("/sys/admin/get_all_topics")
	TopicGetAllNodes  = NewTopic("/sys/admin/get_all_nodes")
	TopicStopNode     = NewTopic("/sys/admin/stop_node")

	// 日志
	TopicSysLog = NewTopic("/sys/log")
	// 连接权限验证
	TopicAuth = NewTopic("/sys/auth")
)

var (
	ErrNotAllowedTopic = errors.New("this topic is not allowed to subscribe or publish")
	ErrUnformedMsg     = errors.New("unformed Msg")
)

func TypeofTopic(t Topic) string {
	switch t.FirstFragment() {
	case SysTopic:
		return SysTopic
	case InnerTopic:
		return InnerTopic
	default:
		return PublicTopic
	}
}

func IsPublicTopic(t Topic) bool {
	return TypeofTopic(t) == PublicTopic
}

func IsInnerTopic(t Topic) bool {
	return TypeofTopic(t) == InnerTopic
}

func IsSysTopic(t Topic) bool {
	return TypeofTopic(t) == SysTopic
}

func NewTopic(t string) Topic {
	if len(t) > 0 && t[0] == '/' {
		return topic(t)
	}
	for _, c := range t {
		if c == messageSeparatorByte {
			panic(InvalidTopic)
		}
	}
	return topic("/" + t)
}

type Topic interface {
	String() string
	Bytes() []byte
	FirstFragment() string
	Fragment(int) string
	Since(int) string
	Len() int
}

type topic []byte

func (t topic) String() string {
	return string(t)
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

type (
	MsgType = byte
)

func MsgTypeName(m MsgType) string {
	switch m {
	case MsgTypeString:
		return "string"
	case MsgTypeInt:
		return "int"
	case MsgTypeBool:
		return "bool"
	case MsgTypeBytes:
		return "[]byte"
	case MsgTypeJSON:
		return "json"
	default:
		return "Invalid(" + string(m) + ")"
	}
}

// The same values are exists on client side too.
const (
	MsgTypeString MsgType = '0'
	MsgTypeInt    MsgType = '1'
	MsgTypeBool   MsgType = '2'
	MsgTypeBytes  MsgType = '3'
	MsgTypeJSON   MsgType = '4'
)

const (
	messageSeparator     = ";"
	messageSeparatorByte = ';'
)

var InvalidMessage = errors.New("invalid message")
var InvalidTopic = errors.New("invalid topic")

type Serializer struct {
	prefix    []byte
	prefixLen int
	typeIdx   int
	randomIdx int
	sourceIdx int
	targetIdx int

	buf sync.Pool
}

// 格式: prefix(n)type(1)random_tag(4)source_idx(4)target_topic;msg
func NewSerializer(messagePrefix []byte) *Serializer {
	typeIdx := len(messagePrefix)
	randomIdx := typeIdx + 1
	sourceIdx := randomIdx + 4
	targetIdx := sourceIdx + 4
	return &Serializer{
		prefix:    messagePrefix,
		prefixLen: typeIdx,
		typeIdx:   typeIdx,
		randomIdx: randomIdx,
		sourceIdx: sourceIdx,
		targetIdx: targetIdx,
		buf: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 10240)
				copy(b, messagePrefix)
				return b
			},
		},
	}
}

var (
	boolTrue  = []byte("1")
	boolFalse = []byte("0")
)

func (ms *Serializer) ReturnBackBytes(p []byte) {

	ms.buf.Put(p[:10240])
}

// websocketMessageSerialize serializes a custom websocket message from websocketServer to be delivered to the client
// returns the  string form of the message
// Supported data types are: string, int, bool, bytes and JSON.
var msgCounter int32 = 0

// 格式: prefix(n)type(1)random_tag(4)source_idx(4)target_topic;msg
func (ms *Serializer) Serialize(t Topic, data interface{}) ([]byte, error) {
	b := ms.buf.Get().([]byte)
	// 省略空source_topic source_topic 一般为空 只有server对消息进行转发时自动追加消息
	// random_tag 记录了一个序列化的次数
	binary.BigEndian.PutUint32(b[ms.randomIdx:ms.sourceIdx], uint32(atomic.AddInt32(&msgCounter, 1)))
	// source_idx 此时需要为空, 只有经过一个节点进行消息转发时更新此记录
	if data == nil {
		data = ""
	}
	tb := append(t.Bytes(), messageSeparatorByte)
	msgLen := ms.targetIdx + len(tb)
	copy(b[ms.targetIdx:], tb)
	switch v := data.(type) {
	case string:
		b[ms.typeIdx] = MsgTypeString
		copy(b[msgLen:], v)
		msgLen += len(v)
	case int:
		b[ms.typeIdx] = MsgTypeInt
		//s := strconv.Itoa(v)
		s := strconv.AppendInt(b[msgLen:msgLen], int64(v), 10)
		//copy(b[msgLen:], s)
		msgLen += len(s)
	case bool:
		b[ms.typeIdx] = MsgTypeBool
		if v {
			b[msgLen] = '1'
		} else {
			b[msgLen] = '0'
		}
		msgLen += 1
	case []byte:
		b[ms.typeIdx] = MsgTypeBytes
		copy(b[msgLen:], v)
		msgLen += len(v)
	default:
		//we suppose is json
		res, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		copy(b[msgLen:], res)
		msgLen += len(res)
		b[ms.typeIdx] = MsgTypeJSON
	}
	return b[:msgLen], nil
}

// deserialize deserializes a custom websocket message from the client
// such as  prefix;topic;0;abc_msg
// Supported data types are: string, int, bool, bytes and JSON.
// 格式: prefix(n)type(1)random_tag(4)source_idx(4)target_topic;msg
func (ms *Serializer) Deserialize(websocketMessage []byte) (interface{}, MsgType, error) {
	if len(websocketMessage) < ms.targetIdx {
		return nil, ' ', InvalidMessage
	}
	typ := websocketMessage[ms.typeIdx]
	var data []byte
	for i, c := range websocketMessage[ms.targetIdx:] {
		if c == messageSeparatorByte {
			data = websocketMessage[ms.targetIdx+i+1:]
			break
		}
	}
	switch typ {
	case MsgTypeString:
		return *(*string)(unsafe.Pointer(&data)), MsgTypeString, nil
	case MsgTypeInt:
		msg, err := strconv.Atoi(*(*string)(unsafe.Pointer(&data)))
		if err != nil {
			log.HandlerErrs(err)
			return nil, ' ', InvalidMessage
		}
		return msg, MsgTypeInt, nil
	case MsgTypeBool:
		if bytes.Equal(data, boolTrue) {
			return true, MsgTypeBool, nil
		}
		return false, MsgTypeBool, nil
	case MsgTypeBytes:
		return data, MsgTypeBytes, nil
	case MsgTypeJSON:
		return data, MsgTypeJSON, nil
		//var msg interface{}
		//err := json.Unmarshal(data, &msg)
		//return msg, err
	default:
		return nil, ' ', InvalidMessage
	}
}

func (ms *Serializer) SeparateMessage(msg []byte) (target Topic, source []byte, tag string, typ MsgType, data []byte) {
	sepIdx := 0
	startIdx := 0
	for i, c := range msg {
		if c == messageSeparatorByte {
			switch sepIdx {
			case 1:
				target = NewTopic(string(msg[startIdx:i]))
			case 2:
				source = msg[startIdx:i]
			case 3:
				tag = string(msg[startIdx:i])
			case 4:
				typ = msg[startIdx]
				data = msg[i+1:]
				return
			}
			sepIdx++
			startIdx = i + 1
		}
	}
	return
}

// getWebsocketCustomEvent return empty string when the websocketMessage is native message
// 格式: prefix;target_topic;source_topic;random_tag;type;msg
func (ms *Serializer) GetMsgTopic(websocketMessage []byte) Topic {
	if len(websocketMessage) < ms.targetIdx {
		return nil
	}
	t := ""
	for _, c := range websocketMessage[ms.targetIdx:] {
		if c == messageSeparatorByte {
			break
		}
		t += string(c)
	}
	return NewTopic(t)
}

func (ms *Serializer) GetSourceTopic(msg []byte) Topic {
	sepIdx := 0
	startIdx := 0
	for i, c := range msg {
		if c == messageSeparatorByte {
			if sepIdx == 2 {
				return NewTopic("/self/" + string(msg[startIdx:i]))
			}
			sepIdx++
			startIdx = i + 1
		}
	}
	return nil
}

func (ms *Serializer) AddSourceTopicItem(msg []byte, source_id string) {
	// TODO
}
