package cmd

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	client "github.com/lightjiang/webds/client/go.client"
	"github.com/lightjiang/webds/message"
	"github.com/urfave/cli"
	"strconv"
	"time"
)

var Topic = cli.Command{
	Name:         "topic",
	ShortName:    "",
	Aliases:      nil,
	Usage:        "topic",
	UsageText:    "",
	Description:  "some command about topic",
	BashComplete: nil,
	Action:       nil,
	Subcommands: []cli.Command{
		sub,
		pub,
		list,
	},
	Flags: nil,
}

var sub = cli.Command{
	Name:   "sub",
	Usage:  "subscribe topics",
	Action: runSub,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "hide",
			Usage: "hide the msg output",
		},
	},
}

func runSub(c *cli.Context) error {
	conn := newConn(c)
	fc := func(t string) {
		conn.Subscribe(t, func(data interface{}) {
			if !c.Bool("hide") {
				fmt.Printf("%v %s > %#v \n", time.Now(), t, data)
			}
		})
	}
	for _, i := range c.Args() {
		log.Debug().Msg("subscribe " + i)
		fc(i)
	}
	return conn.Wait()
}

var pub = cli.Command{
	Name:   "pub",
	Usage:  "publish a message",
	Action: runPub,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "hide",
			Usage: "hide the msg output",
		},
		cli.StringFlag{
			Name:  "type",
			Usage: "message type, int/str.",
			Value: "str",
		},
		cli.IntFlag{
			Name:        "times",
			Usage:       "times",
			Value:       1,
			Destination: nil,
		},
		cli.DurationFlag{
			Name:        "delta",
			Value:       time.Second,
			Destination: nil,
		},
	},
}

func runPub(c *cli.Context) error {
	arg := c.Args()
	if len(arg) != 2 {
		log.Warn().Msg("please add topic and msg")
		return nil
	}
	conn := newConn(c)
	conn.OnConnect(func() {
		var msg interface{}
		msg = arg[1]
		if c.String("type") == "int" {
			var err error
			msg, err = strconv.Atoi(arg[1])
			if err != nil {
				log.Warn().Err(err).Msg(arg[1] + " can't be converted to number. ")
				log.HandlerErrs(conn.Close())
				return
			}
		}
		for i := 0; i < c.Int("times"); i++ {
			if i != 0 {
				time.Sleep(c.Duration("delta"))
			}
			if !c.Bool("hide") {
				fmt.Printf("%v %s < %s\n", time.Now(), arg[0], msg)
			}
			log.HandlerErrs(conn.Pub(arg[0], msg))
		}
		log.HandlerErrs(conn.Close())
	})
	return conn.Wait()
}

var list = cli.Command{
	Name:        "list",
	Usage:       "",
	Description: "show all the topic",
	Action:      runList,
}

func runList(c *cli.Context) error {
	conn := newConn(c)
	conn.OnConnect(func() {
		log.HandlerErrs(conn.Pub(message.TopicGetAllTopics.String(), ""))
	})
	conn.Subscribe(message.TopicGetAllTopics.String(), func(data string) {
		fmt.Print(data)
		conn.Close()
	})
	return conn.Wait()
}

func newConn(c *cli.Context) client.Connection {
	return client.New(&client.Config{
		Host:            c.GlobalString("host"),
		ID:              c.GlobalString("id"),
		PingPeriod:      0,
		MaxMessageSize:  0,
		BinaryMessages:  false,
		ReadBufferSize:  0,
		WriteBufferSize: 0,
	})
}
