package core

import "testing"

func TestEncodeUrl(t *testing.T) {
	var url = ""
	cases := [][]interface{}{
		{"http://127.0.0.1", 80, "", "ws://127.0.0.1"},
		{"127.0.0.1", 800, "", "ws://127.0.0.1:800"},
		{"127.0.0.1", 1080, "/", "ws://127.0.0.1:1080"},
		{"127.0.0.1", 1080, "asd1", "ws://127.0.0.1:1080/asd1"},
		{"127.0.0.1", 1080, "/asd1/", "ws://127.0.0.1:1080/asd1/"},
	}
	for _, c := range cases {
		url = EncodeUrl(c[0].(string), uint(c[1].(int)), c[2].(string))
		if url != c[3].(string) {
			t.Errorf("encode url error: %s,%d,%s ->%s", c[0], c[1], c[2], url)
		}
	}
}
func TestDecodeUrl(t *testing.T) {
	var host = ""
	var port uint
	var path string
	cases := [][]interface{}{
		{"127.0.0.1", 80, "/", "127.0.0.1"},
		{"127.0.0.1", 800, "/", "http://127.0.0.1:800"},
		{"127.0.0.1", 1080, "/", "ws://127.0.0.1:1080/"},
		{"127.0.0.1", 1080, "/asd1", "ws://127.0.0.1:1080/asd1"},
		{"127.0.0.1", 1080, "/asd1/", "ws://127.0.0.1:1080/asd1/"},
	}
	for _, c := range cases {
		host, port, path = DecodeUrl(c[3].(string))
		if host != c[0].(string) || port != uint(c[1].(int)) || path != c[2].(string) {
			t.Errorf("encode url error: %s %d %s (%s)", host, port, path, c[3])
		}
	}
}
