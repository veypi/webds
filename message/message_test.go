package message

import "testing"

func TestSerializer(t *testing.T) {
	s := NewSerializer([]byte("ws:"))
	data := map[int]interface{}{
		0: "test",
		1: 112345,
		2: -1,
		3: true,
		4: []byte("abc"),
		5: map[string]interface{}{"a": "abc"},
	}
	tp := NewTopic("home")
	for i, d := range data {
		res, err := s.Serialize(tp, d)
		if err != nil {
			t.Error(err)
		}
		r, err := s.Deserialize(tp, res)
		if err != nil {
			t.Error(err)
		}
		if i == 5 {
			r = string(r.([]byte))
		}
		t.Logf("%v (%T) => %v (%T)", d, d, r, r)
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
		_, err = s.Deserialize(tp, res)
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
