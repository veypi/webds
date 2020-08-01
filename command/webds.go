package main

import (
	"github.com/urfave/cli/v2"
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
	Host:     "ws://127.0.0.1:10086",
	LogLevel: "info",
}

const appName = "webds"

var cfgPath = cmd.GetCfgPath(appName, appName)

func main() {
	cmd.LoadCfg(cfgPath, cfg)
	app := cmd.NewCli(appName, cfg, cfgPath)
	app.Version = webds.Version
	app.Usage = "webds command tool, which is valid in server's host."
	app.Flags = append(app.Flags,
		&cli.StringFlag{
			Name:        "log_level",
			Usage:       "trace/debug/info/warn/error/fatal/panic",
			Value:       cfg.LogLevel,
			Destination: &cfg.LogLevel,
		},
		&cli.StringFlag{
			Name:        "host",
			Usage:       "the server address",
			Value:       cfg.Host,
			Destination: &cfg.Host,
		},
		&cli.StringFlag{
			Name:        "id",
			Aliases:     []string{"i"},
			Usage:       "the client id",
			Value:       cfg.ID,
			Destination: &cfg.ID,
		},
	)
	app.Commands = append(app.Commands,
		Topic,
		Node,
		Cluster,
		Scan,
	)
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
