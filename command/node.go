package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/message"
)

var Node = &cli.Command{
	Name:        "node",
	Usage:       "webds node list/stop",
	Description: "command about node",
	Subcommands: []*cli.Command{
		{
			Name:   "list",
			Usage:  "webds node list",
			Action: runNodeList,
			Flags:  nil,
		},
		{
			Name:   "stop",
			Usage:  "webds node stop node1 node2 ...",
			Action: runStopNode,
		},
	},
	Flags: nil,
}

func runNodeList(c *cli.Context) error {
	conn, err := newConn()
	if err != nil {
		return err
	}
	conn.OnConnect(func() {
		conn.Echo(message.TopicGetAllNodes, "")
	})
	conn.Subscribe(message.TopicGetAllNodes, func(data interface{}) {
		fmt.Print(data)
		log.HandlerErrs(conn.Close())
	})
	return conn.Wait()
}

func runStopNode(c *cli.Context) error {
	if c.Args().Len() == 0 {
		log.Warn().Msg("missing node_id")
		return nil
	}
	conn, err := newConn()
	if err != nil {
		return err
	}
	conn.OnConnect(func() {
		for _, v := range c.Args().Slice() {
			conn.Echo(message.TopicStopNode, v)
		}
		log.HandlerErrs(conn.Close())
	})
	return conn.Wait()
}
