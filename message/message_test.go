package message

import "testing"

func TestSerializer(t *testing.T) {
	s := NewSerializer([]byte("ws"))
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
			t.Errorf("%s :%v", res, err)
			return
		}
		if i == 5 {
			r = string(r.([]byte))
		}
		t.Logf("%d: %v (%T) => %v (%T)", i, d, d, r, r)
	}

}

func BenchmarkSerializer(b *testing.B) {
	s := NewSerializer([]byte("ws"))
	data := map[string]interface{}{"a": "b", "c": 2, "d": 1.5}
	//data := 1234
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
	}
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
	s := NewSerializer([]byte("ws"))
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
	s := NewSerializer([]byte("ws"))
	st := s.GetSourceTopic([]byte("ws;target_topic;source_topic;random_tag;type;msg"))
	if st.String() != "/source_topic" {
		t.Errorf("get source topic error: %s", st)
	}
	//s.SeparateMessage(data)
}
