package message

import (
	"testing"
)

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
		res, err := Encode(tp, d)
		if err != nil {
			t.Error(err)
			return
		}
		r, err := Decode(res)
		if err != nil {
			t.Errorf("%d %s :%v", i, res, err)
			return
		}
		t.Logf("%d %s : %v (%T) => %v (%T)", i, res, d, d, string(r.Data), r.Data)
		r.Release()
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
		res, err = Encode(tp, data)
		if err != nil {
			b.Error(err)
			return
		}
		_, err = Decode(res)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

/*
goos: linux
goarch: amd64
pkg: github.com/veypi/webds/message v0.2.5
BenchmarkSerializer_Int-16               1000000              1797 ns/op           10285 B/op          3 allocs/op
BenchmarkSerializer_String-16             506079              2371 ns/op           10293 B/op          3 allocs/op
BenchmarkSerializer_Bool-16               524312              2301 ns/op           10277 B/op          2 allocs/op
BenchmarkSerializer_bytes-16              491308              2369 ns/op           10309 B/op          3 allocs/op
BenchmarkSerializer_Json-16               320527              6259 ns/op           10485 B/op          7 allocs/op


goos: linux
goarch: amd64
pkg: github.com/veypi/webds/message protobuf
BenchmarkSerializer_Int-16               1332846               861 ns/op             320 B/op          7 allocs/op
BenchmarkSerializer_String-16            1381249              1007 ns/op             312 B/op          6 allocs/op
BenchmarkSerializer_Bool-16              1410748               948 ns/op             304 B/op          6 allocs/op
BenchmarkSerializer_bytes-16             1467572               870 ns/op             304 B/op          5 allocs/op
BenchmarkSerializer_Json-16               836677              1506 ns/op             509 B/op          9 allocs/op
PASS
ok      github.com/veypi/webds/message  9.916s

*/
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
