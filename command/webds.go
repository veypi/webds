package main

import (
	"github.com/urfave/cli/v2"
	"github.com/veypi/utils"
	"github.com/veypi/utils/cmd"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds"
	client "github.com/veypi/webds/client/go.client"
	"os"
)

var cfg = &struct {
	LogLevel string
	Host     string
	ID       string
}{
	ID:       "admin",
	Host:     "ws://127.0.0.1:8080",
	LogLevel: "info",
}

const appName = "webds"

var cfgPath = utils.PathJoin(cmd.GetLocalCfg(appName), appName+".yml")

func main() {
	err := cmd.LoadCfg(cfgPath, cfg)
	if err != nil && !os.IsNotExist(err) {
		log.Warn().Msg(err.Error())
	}
	app := cli.NewApp()
	app.Version = webds.Version
	app.Name = appName
	app.Usage = "webds command tool, which is valid in server's host."
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "log_level",
			Usage:       "trace/debug/info/warn/error/fatal/panic, default is info level",
			Value:       cfg.LogLevel,
			Destination: &cfg.LogLevel,
		},
		&cli.StringFlag{
			Name:        "host",
			Usage:       "the server address, default is ws://127.0.0.1:8080",
			Value:       cfg.Host,
			Destination: &cfg.Host,
		},
		&cli.StringFlag{
			Name:        "id",
			Usage:       "the client id",
			Value:       cfg.ID,
			Destination: &cfg.ID,
		},
	}
	app.Commands = []*cli.Command{
		Topic,
		Node,
		Cluster,
	}
	err = cmd.NewCli(app, cfg, cfgPath)
	if err != nil {
		log.Warn().Msg(err.Error())
	}
	app.Before = func(c *cli.Context) error {
		//log.DisableCaller()
		if l, err := log.ParseLevel(cfg.LogLevel); err != nil {
			log.Warn().Str("level", cfg.LogLevel).Msg("log level is not right")
		} else {
			log.SetLevel(l)
			log.Debug().Msg("set log level to " + l.String())
		}
		return nil
	}

	_ = app.Run(os.Args)
}

func newConn() (webds.Connection, error) {
	return client.NewFromUrl(cfg.ID, cfg.Host, nil)
}
