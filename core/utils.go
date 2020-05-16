package core

import (
	"github.com/lightjiang/utils/log"
	"strconv"
	"strings"
)

func DecodeUrl(url string) (host string, port uint, path string) {
	host = url
	if i := strings.Index(url, "//"); i >= 0 {
		host = url[i+2:]
	}
	path = "/"
	if i := strings.IndexByte(host, '/'); i >= 0 {
		path = host[i:]
		host = host[:i]
	}
	port = 80
	if i := strings.IndexByte(host, ':'); i >= 0 {
		p, err := strconv.Atoi(host[i+1:])
		if err != nil {
			log.Warn().Msg(err.Error() + "(parse url err):" + url)
		} else {
			port = uint(p)
		}
		host = host[:i]
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return
}

func EncodeUrl(host string, port uint, path string) string {
	if i := strings.Index(host, "//"); i >= 0 {
		host = host[i+2:]
	}
	url := "ws://" + host
	if port > 0 && port != 80 {
		url += ":" + strconv.Itoa(int(port))
	}
	if path != "" && path != "/" {
		if path[0] != '/' {
			path = "/" + path
		}
		url += path
	}
	return url
}
