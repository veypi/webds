package client

import (
	"github.com/veypi/webds"
	"github.com/veypi/webds/conn"
	"github.com/veypi/webds/core"
)

var defaultCfg = &webds.Config{
	ID:             "",
	IDGenerator:    nil,
	SuperiorMaster: nil,
	LateralMaster:  nil,
	EnableCluster:  false,
	MsgPrefix:      nil,
}

func New(id string, host string, port uint, path string, cfg core.ConnCfg) (core.Connection, error) {
	if cfg == nil {
		cfg = defaultCfg
	}
	return conn.NewActiveConn(id, host, port, path, cfg)
}

func NewFromUrl(id string, url string, cfg core.ConnCfg) (core.Connection, error) {
	host, port, path := core.DecodeUrl(url)
	return New(id, host, port, path, cfg)
}
