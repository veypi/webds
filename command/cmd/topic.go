package cmd

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	client "github.com/lightjiang/webds/client/go.client"
	"github.com/lightjiang/webds/core"
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
		cli.Int64Flag{
			Name:   "max",
			Usage:  "max limited to receive msg",
			Hidden: false,
			Value:  0,
		},
	},
}

func runSub(c *cli.Context) error {
	conn, err := newConn(c)
	if err != nil {
		return err
	}
	fc := func(t string) {
		step := make([]int64, 0, 1000000)
		hide := c.Bool("hide")
		max := c.Int("max")
		log.Info().Msg("start subscribe " + t)
		conn.Subscribe(message.NewTopic(t), func(data interface{}) {
			step = append(step, time.Now().UnixNano())
			if !hide {
				fmt.Printf("%s %s  %d > %#v \n", time.Now().Format("2006-01-02 15:04:05"), t, len(step), data)
			}
			if len(step) == max {
				conn.Close()
				return
			}
		})
		conn.OnDisconnect(func() {
			l := len(step)
			if l > 0 {
				start := time.Unix(0, step[0])
				end := time.Unix(0, step[l-1])
				log.Info().Msgf("%s: %s -> %s: receive %d msg for %d ms", t, start, end, len(step), end.Sub(start).Milliseconds())
			}
		})
	}
	for _, i := range c.Args() {
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
	conn, err := newConn(c)
	if err != nil {
		return err
	}
	fc := func() {
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
		now := time.Now()
		for i := 0; i < c.Int("times"); i++ {
			if i != 0 {
				time.Sleep(c.Duration("delta"))
			}
			if !c.Bool("hide") {
				fmt.Printf("%s %s < %s\n", time.Now().Format("2006-01-02 15:04:05"), arg[0], msg)
			}
			conn.Publisher(arg[0])(msg)
			if !conn.Alive() {
				return
			}
		}
		if c.Int("times") > 10 {
			log.Info().Msgf("send %d msg for %s", c.Int("times"), time.Now().Sub(now).String())
		}
		log.HandlerErrs(conn.Close())
	}
	conn.OnConnect(func() {
		fc()
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
	conn, err := newConn(c)
	if err != nil {
		return err
	}
	conn.OnConnect(func() {
		conn.Echo(message.TopicGetAllTopics, "")
	})
	conn.Subscribe(message.TopicGetAllTopics, func(data string) {
		fmt.Println(data)
		conn.Close()
	})
	return conn.Wait()
}

func newConn(c *cli.Context) (core.Connection, error) {
	id := c.GlobalString("id")
	return client.NewFromUrl(id, c.GlobalString("host"), nil)
}
