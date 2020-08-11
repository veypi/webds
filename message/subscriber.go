package message

import (
	"github.com/veypi/utils"
	"github.com/veypi/utils/log"
)

func newSubscriber(callback Func) *Subscriber {
	switch callback.(type) {
	case FuncBlank:
	case FuncInt:
	case FuncString:
	case FuncBytes:
	case FuncDefault:
	case FuncBool:
	case FuncError:
	case RawFunc:
	default:
		panic("invalid callback function")
	}
	return &Subscriber{callback: callback}
}

type Subscriber struct {
	callback   Func
	counter    uint
	counterMax uint
	CallPath   string
	utils.FastLocker
}

func (s *Subscriber) Valid() bool {
	if s.callback == nil || (s.counterMax > 0 && s.counter >= s.counterMax) {
		return false
	}
	return true
}

func (s *Subscriber) SetMax(i uint) {
	s.counterMax = i
}

func (s *Subscriber) SetOnce() {
	s.counterMax = 1
}
func (s *Subscriber) Cancel() {
	s.callback = nil
}

func (s *Subscriber) Do(apd interface{}) {
	s.Lock()
	defer func() {
		s.Unlock()
		if e := recover(); e != nil {
			log.Error().Err(nil).Msgf("%v: ", e, s.CallPath)
		}
	}()
	if !s.Valid() {
		return
	}
	s.counter++
	m, ok := apd.(*Message)
	if ok {
		if cb, ok := s.callback.(RawFunc); ok {
			cb(m)
			return
		}
		apd = m.Body()
	}
	switch cb := s.callback.(type) {
	case FuncBlank:
		cb()
	case FuncBool:
		cb(apd.(bool))
	case FuncBytes:
		cb(apd.([]byte))
	case FuncDefault:
		cb(apd)
	case FuncInt:
		cb(apd.(int))
	case FuncString:
		cb(apd.(string))
	case FuncError:
		cb(apd.(error))
	case RawFunc:
		cb(apd.(*Message))
	default:
		panic("it should occur")
	}
}

func NewSubscriberList() *SubscriberList {
	l := &SubscriberList{}
	return l
}

type SubscriberList struct {
	core []*Subscriber
}

func (l *SubscriberList) Add(cb Func) *Subscriber {
	s := newSubscriber(cb)
	if l.core == nil {
		l.core = make([]*Subscriber, 0, 10)
	}
	l.core = append(l.core, s)
	s.CallPath = utils.CallPath(2)
	return s
}

func (l *SubscriberList) Range(cb func(*Subscriber)) {
	for _, s := range l.core {
		cb(s)
	}
}

func (l *SubscriberList) Len() int {
	return len(l.core)
}
