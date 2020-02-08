package webds

import "testing"

func TestDefaultIDGenerator(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Log(DefaultIDGenerator(nil))
	}
}

func BenchmarkDefaultIDGenerator(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DefaultIDGenerator(nil)
	}
}
