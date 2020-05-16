package cmd

import (
	"fmt"
	"github.com/lightjiang/webds/message"
	"github.com/urfave/cli"
)

var Cluster = cli.Command{
	Name:         "cluster",
	ShortName:    "",
	Aliases:      nil,
	Usage:        "cluster",
	UsageText:    "",
	Description:  "some command about cluster",
	BashComplete: nil,
	Action:       nil,
	Subcommands: []cli.Command{
		info,
	},
	Flags: nil,
}

var info = cli.Command{
	Name:  "info",
	Usage: "info about cluster",
	Action: func(c *cli.Context) error {
		conn, err := newConn(c)
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
