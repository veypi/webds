package cmd

import (
	"fmt"
	"github.com/urfave/cli"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/message"
	"time"
)

var Node = cli.Command{
	Name:        "node",
	Usage:       "webds node list/stop",
	Description: "command about node",
	Subcommands: []cli.Command{
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
	conn, err := newConn(c)
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
	if len(c.Args()) == 0 {
		log.Warn().Msg("missing node_id")
		return nil
	}
	conn, err := newConn(c)
	if err != nil {
		return err
	}
	conn.OnConnect(func() {
		log.Warn().Msgf("%s stop ", time.Now())
		for _, v := range c.Args() {
			conn.Echo(message.TopicStopNode, v)
		}
		log.Warn().Msgf("%s stop ", time.Now())
		log.HandlerErrs(conn.Close())
	})
	return conn.Wait()
}
