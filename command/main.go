package main

import (
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds"
	"github.com/lightjiang/webds/command/cmd"
	"github.com/urfave/cli"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Version = webds.Version
	app.Name = "webds"
	app.Usage = "webds command tool"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "log_level",
			Usage: "trace/debug/info/warn/error/fatal/panic, default is info level",
			Value: "info",
		},
	}
	app.Commands = []cli.Command{
		cmd.Topic,
	}
	app.Before = func(c *cli.Context) error {
		logLevel := c.String("log_level")
		if l, err := log.ParseLevel(logLevel); err != nil {
			log.Warn().Str("level", logLevel).Msg("log level is not right")
		} else {
			log.SetLevel(l)
			log.Debug().Msg("set log level to " + l.String())
		}
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Error().Msg(err.Error())
	}
}
