package message

import (
	"github.com/veypi/utils"
	"github.com/veypi/utils/log"
)

type Subscriber interface {
	Do(data interface{})
	Valid() bool
	SetMax(uint)
	SetOnce()
	Cancel()
}

var _ Subscriber = &subscriber{}

func newSubscriber(callback Func) *subscriber {
	switch callback.(type) {
	case FuncBlank:
	case FuncInt:
	case FuncString:
	case FuncBytes:
	case FuncDefault:
	case FuncBool:
	case FuncError:
	default:
		panic("invalid callback function")
	}
	return &subscriber{callback: callback}
}

type subscriber struct {
	callback   Func
	counter    uint
	counterMax uint
	utils.FastLocker
}

func (s *subscriber) Valid() bool {
	if s.callback == nil || (s.counterMax > 0 && s.counter >= s.counterMax) {
		return false
	}
	return true
}

func (s *subscriber) SetMax(i uint) {
	s.counterMax = i
}

func (s *subscriber) SetOnce() {
	s.counterMax = 1
}
func (s *subscriber) Cancel() {
	s.callback = nil
}

func (s *subscriber) Do(data interface{}) {
	s.Lock()
	defer func() {
		s.Unlock()
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%v", e)
		}
	}()
	if !s.Valid() {
		return
	}
	s.counter++
	switch cb := s.callback.(type) {
	case FuncBlank:
		cb()
	case FuncBool:
		cb(data.(bool))
	case FuncBytes:
		cb(data.([]byte))
	case FuncDefault:
		cb(data)
	case FuncInt:
		cb(data.(int))
	case FuncString:
		cb(data.(string))
	case FuncError:
		cb(data.(error))
	default:
		panic("it should occur")
	}
}

type SubscriberList interface {
	Add(callback Func) Subscriber
	Range(func(s Subscriber))
	Len() int
}

func NewSubscriberList() SubscriberList {
	l := &subscriberList{}
	return l
}

type subscriberList struct {
	core []Subscriber
}

func (l *subscriberList) Add(cb Func) Subscriber {
	s := newSubscriber(cb)
	if l.core == nil {
		l.core = make([]Subscriber, 0, 10)
	}
	l.core = append(l.core, s)
	return s
}

func (l *subscriberList) Range(cb func(Subscriber)) {
	for _, s := range l.core {
		cb(s)
	}
}

func (l *subscriberList) Len() int {
	return len(l.core)
}
