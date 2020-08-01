package client

import (
	"github.com/veypi/webds"
	"github.com/veypi/webds/cfg"
	"github.com/veypi/webds/conn"
)

var defaultCfg = &webds.Config{
	ID:            "",
	IDGenerator:   nil,
	EnableCluster: false,
}

func New(id string, host string, port uint, path string, cfg *cfg.Config) (webds.Connection, error) {
	if cfg == nil {
		cfg = defaultCfg
	}
	return conn.NewActiveConn(id, host, port, path, cfg)
}

func NewFromUrl(id string, url string, cfg *cfg.Config) (webds.Connection, error) {
	host, port, path := webds.DecodeUrl(url)
	return New(id, host, port, path, cfg)
}
