package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/veypi/webds/message"
)

var Cluster = &cli.Command{
	Name:         "cluster",
	Aliases:      nil,
	Usage:        "cluster",
	UsageText:    "",
	Description:  "some command about cluster",
	BashComplete: nil,
	Action:       nil,
	Subcommands: []*cli.Command{
		&info,
	},
	Flags: nil,
}

var info = cli.Command{
	Name:  "info",
	Usage: "info about cluster",
	Action: func(c *cli.Context) error {
		conn, err := newConn()
		if err != nil {
			return err
		}
		conn.OnConnect(func() {
			conn.Echo(message.TopicClusterInfo, "")
		})
		conn.Subscribe(message.TopicClusterInfo, func(s string) {
			fmt.Println(s)
			conn.Close()
		})
		return conn.Wait()
	},
	Flags: []cli.Flag{},
}
