package message

import (
	"bytes"
	"github.com/json-iterator/go"
	"strconv"
	"strings"
	"sync"

	"errors"
	"fmt"
)

var json = jsoniter.ConfigFastest

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

type Topic string

func (t Topic) String() string {
	return string(t)
}

func (t Topic) FirstFragment() string {
	return t.Fragment(0)
}

func (t Topic) Fragment(count uint) string {
	var res string
	var tempCount uint
	for _, i := range strings.TrimPrefix(t.String(), "/") {
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

func (t Topic) Since(count int) string {
	var index int
	var tempCount int
	if t.String()[0] == '/' {
		tempCount = -1
	}
	for i, v := range t.String() {
		if v == '/' {
			if tempCount == count {
				break
			}
			tempCount++
			index = i + 1
		}
	}
	return t.String()[index:]
}

var messageSeparatorByte = messageSeparator[0]

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
func (ms *Serializer) Serialize(event string, data interface{}) ([]byte, error) {
	b := ms.buf.Get().(*bytes.Buffer)
	//b := &bytes.Buffer{}
	b.Write(ms.prefix)
	b.WriteString(event)
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
func (ms *Serializer) Deserialize(event string, websocketMessage []byte) (interface{}, error) {
	dataStartIdx := ms.prefixAndSepIdx + len(event) + 3
	if len(websocketMessage) <= dataStartIdx {
		return nil, errors.New("websocket invalid message: " + string(websocketMessage))
	}

	typ := messageType(websocketMessage[ms.prefixAndSepIdx+len(event)+1 : ms.prefixAndSepIdx+len(event)+2]) // in order to go-websocket-message;user;-> 4

	data := websocketMessage[dataStartIdx:]

	switch typ {
	case messageTypeString:
		return string(data), nil
	case messageTypeInt:
		msg, err := strconv.Atoi(string(data))
		if err != nil {
			return nil, err
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
		return nil, errors.New(fmt.Sprintf("Type %s is invalid for message: %s", typ.Name(), websocketMessage))
	}
}

// getWebsocketCustomEvent return empty string when the websocketMessage is native message
func (ms *Serializer) GetMsgTopic(websocketMessage []byte) Topic {
	if len(websocketMessage) < ms.prefixAndSepIdx {
		return ""
	}
	s := websocketMessage[ms.prefixAndSepIdx:]
	return Topic(s[:bytes.IndexByte(s, messageSeparatorByte)])
}
