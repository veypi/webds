package message

import (
	"encoding/binary"
	"testing"
)

var s = NewSerializer([]byte("ws"))

func TestSerializer(t *testing.T) {
	data := map[int]interface{}{
		-1: nil,
		0:  "test",
		1:  112345,
		2:  -1,
		3:  true,
		4:  []byte("abc"),
		5:  map[string]interface{}{"a": "abc"},
	}
	tp := NewTopic("home")
	for i, d := range data {
		res, err := s.Serialize(tp, d)
		if err != nil {
			t.Error(err)
			return
		}
		r, _, err := s.Deserialize(res)
		if err != nil {
			t.Errorf("%d %s :%v", i, res, err)
			return
		}
		if i == 5 {
			r = string(r.([]byte))
		}
		s.ReturnBackBytes(res)
		t.Logf("%d %s : %v (%T) => %v (%T)", i, res, d, d, r, r)
	}

}

// BenchmarkSerializer-16           1721188               689 ns/op             205 B/op          6 allocs/op
// BenchmarkSerializer-16          7587789        172 ns/op              72 B/op          6 allocs/op
/*
BenchmarkSerializer_Int-16               9517542               106 ns/op              40 B/op          2 allocs/op
BenchmarkSerializer_String-16           13252210                98.7 ns/op            48 B/op          2 allocs/op
BenchmarkSerializer_Bool-16             19768287                80.2 ns/op            32 B/op          1 allocs/op
BenchmarkSerializer_bytes-16            12071730               113 ns/op              64 B/op          2 allocs/op
BenchmarkSerializer_Json-16              2083652               496 ns/op             216 B/op          6 allocs/op
*/
func benchmarkSerializer(b *testing.B, data interface{}) {
	//data := map[string]interface{}{"a": "b", "c": 2, "d": 1.5}
	var res []byte
	var err error
	tp := NewTopic("home")
	for i := 0; i < b.N; i++ {
		res, err = s.Serialize(tp, data)
		if err != nil {
			b.Error(err)
			return
		}
		_, _, err = s.Deserialize(res)
		if err != nil {
			b.Error(err)
			return
		}
		s.ReturnBackBytes(res)
	}
}

func BenchmarkSerializer_Int(b *testing.B) {
	benchmarkSerializer(b, 12345)
}
func BenchmarkSerializer_String(b *testing.B) {
	benchmarkSerializer(b, "12345")
}
func BenchmarkSerializer_Bool(b *testing.B) {
	benchmarkSerializer(b, true)
}
func BenchmarkSerializer_bytes(b *testing.B) {
	benchmarkSerializer(b, []byte("12345"))
}
func BenchmarkSerializer_Json(b *testing.B) {
	benchmarkSerializer(b, map[string]interface{}{"a": "b", "c": 2, "d": 1.5})
}

func TestTopic(t *testing.T) {
	topic := NewTopic("/asd/ss/123/2/435")
	if topic.FirstFragment() != "asd" {
		t.Error("topic firstFragment called error")
	}
	if topic.Fragment(3) != "2" {
		t.Error("topic Fragment called error: " + topic.Fragment(3))
	}
	if topic.Since(3) != "/2/435" {
		t.Error("topic Since called error")
	}
	if topic.Since(0) != "/asd/ss/123/2/435" {
		t.Error(topic.Since(0))
	}
}

func TestSerializer_GetMsgTopic(t *testing.T) {
	tp := NewTopic("home")
	data, err := s.Serialize(tp, 123)
	if err != nil {
		t.Error(err)
		return
	}
	nt := s.GetMsgTopic(data)
	if nt.String() != tp.String() {
		t.Errorf("get msg topic error: %s -> %s", tp, nt)
	}
}

func TestSerializer_SeparateMessage(t *testing.T) {
	st := s.GetSourceTopic([]byte("ws;target_topic;source_topic;random_tag;type;msg"))
	if st.String() != "/self/source_topic" {
		t.Errorf("get source topic error: %s", st)
	}
	p := make([]byte, 6)
	binary.BigEndian.PutUint32(p[1:5], 1)
	t.Log(p)
}
