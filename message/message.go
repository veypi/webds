package message

import (
	"bytes"
	"github.com/json-iterator/go"
	"github.com/lightjiang/utils/log"
	"strconv"
	"sync"

	"errors"
)

var json = jsoniter.ConfigFastest

const (
	PublicTopic = ""
	InnerTopic  = "inner"
	SysTopic    = "sys"
)

var (
	TopicSubscribe    = NewTopic("/sys/subscribe")
	TopicCancel       = NewTopic("/sys/cancel")
	TopicCancelAll    = NewTopic("/sys/cancel_all")
	TopicGetAllTopics = NewTopic("/sys/admin/get_all_topics")
	TopicGetAllNodes  = NewTopic("/sys/admin/get_all_nodes")
	TopicStopNode     = NewTopic("/sys/admin/stop_node")
	TopicSysLog       = NewTopic("/sys/log")
	TopicAuth         = NewTopic("/sys/auth")
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
	return topic("/" + t)
}

type Topic interface {
	String() string
	FirstFragment() string
	Fragment(int) string
	Since(int) string
	Len() int
}

type topic string

func (t topic) String() string {
	return string(t)
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
	messageType string
)

func (m messageType) String() string {
	return string(m)
}

func (m messageType) Name() string {
	switch m {
	case messageTypeString:
		return "string"
	case messageTypeInt:
		return "int"
	case messageTypeBool:
		return "bool"
	case messageTypeBytes:
		return "[]byte"
	case messageTypeJSON:
		return "json"
	default:
		return "Invalid(" + m.String() + ")"
	}
}

// The same values are exists on client side too.
const (
	messageTypeString messageType = "0"
	messageTypeInt    messageType = "1"
	messageTypeBool   messageType = "2"
	messageTypeBytes  messageType = "3"
	messageTypeJSON   messageType = "4"
)

const (
	messageSeparator = ";"
)

var messageSeparatorByte = messageSeparator[0]

var InvalidMessage = errors.New("invalid message")

type Serializer struct {
	prefix []byte

	prefixLen       int
	separatorLen    int
	prefixAndSepIdx int
	prefixIdx       int
	separatorIdx    int

	buf sync.Pool
}

func NewSerializer(messagePrefix []byte) *Serializer {
	return &Serializer{
		prefix:          messagePrefix,
		prefixLen:       len(messagePrefix),
		separatorLen:    len(messageSeparator),
		prefixAndSepIdx: len(messagePrefix) + len(messageSeparator) - 1,
		prefixIdx:       len(messagePrefix) - 1,
		separatorIdx:    len(messageSeparator) - 1,
		buf: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
}

var (
	boolTrueB  = []byte("true")
	boolFalseB = []byte("false")
)

// websocketMessageSerialize serializes a custom websocket message from websocketServer to be delivered to the client
// returns the  string form of the message
// Supported data types are: string, int, bool, bytes and JSON.
func (ms *Serializer) Serialize(t Topic, data interface{}) ([]byte, error) {
	b := ms.buf.Get().(*bytes.Buffer)
	//b := &bytes.Buffer{}
	b.Write(ms.prefix)
	b.WriteString(t.String())
	b.WriteByte(messageSeparatorByte)

	switch v := data.(type) {
	case string:
		b.WriteString(messageTypeString.String())
		b.WriteByte(messageSeparatorByte)
		b.WriteString(v)
	case int:
		b.WriteString(messageTypeInt.String())
		b.WriteByte(messageSeparatorByte)
		b.WriteString(strconv.Itoa(v))
	case bool:
		b.WriteString(messageTypeBool.String())
		b.WriteByte(messageSeparatorByte)
		if v {
			b.Write(boolTrueB)
		} else {
			b.Write(boolFalseB)
		}
	case []byte:
		b.WriteString(messageTypeBytes.String())
		b.WriteByte(messageSeparatorByte)
		b.Write(v)
	default:
		//we suppose is json
		res, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		b.WriteString(messageTypeJSON.String())
		b.WriteByte(messageSeparatorByte)
		b.Write(res)
	}

	message := b.Bytes()
	b.Reset()
	ms.buf.Put(b)
	return message, nil
}

// deserialize deserializes a custom websocket message from the client
// such as  prefix:topic;0:abc_msg
// Supported data types are: string, int, bool, bytes and JSON.
func (ms *Serializer) Deserialize(t Topic, websocketMessage []byte) (interface{}, error) {
	dataStartIdx := ms.prefixAndSepIdx + t.Len() + 3
	if len(websocketMessage) < dataStartIdx {
		return nil, InvalidMessage
	}

	typ := messageType(websocketMessage[ms.prefixAndSepIdx+t.Len()+1 : ms.prefixAndSepIdx+t.Len()+2]) // in order to go-websocket-message;user;-> 4

	data := websocketMessage[dataStartIdx:]

	switch typ {
	case messageTypeString:
		return string(data), nil
	case messageTypeInt:
		msg, err := strconv.Atoi(string(data))
		if err != nil {
			log.HandlerErrs(err)
			return nil, InvalidMessage
		}
		return msg, nil
	case messageTypeBool:
		if bytes.Equal(data, boolTrueB) {
			return true, nil
		}
		return false, nil
	case messageTypeBytes:
		return data, nil
	case messageTypeJSON:
		return data, nil
		//var msg interface{}
		//err := json.Unmarshal(data, &msg)
		//return msg, err
	default:
		return nil, InvalidMessage
	}
}

// getWebsocketCustomEvent return empty string when the websocketMessage is native message
func (ms *Serializer) GetMsgTopic(websocketMessage []byte) Topic {
	if len(websocketMessage) < ms.prefixAndSepIdx {
		return nil
	}
	s := websocketMessage[ms.prefixAndSepIdx:]
	return NewTopic(string(s[:bytes.IndexByte(s, messageSeparatorByte)]))
}
