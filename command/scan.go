package main

import (
	"github.com/urfave/cli/v2"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/libs/scan"
	"strings"
)

var portMin = 10000
var portMax = 10100

var Scan = &cli.Command{
	Name:      "scan",
	ArgsUsage: "scan --max=10000 --min=10100",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:        "max",
			Value:       portMax,
			Destination: &portMax,
		},
		&cli.IntFlag{
			Name:        "min",
			Value:       portMin,
			Destination: &portMin,
		},
	},
	Action: runScan,
}

func runScan(c *cli.Context) error {
	ips := scan.ScanAllIP()
	ips = append(ips, "127.0.0.1")
	log.Warn().Msgf("%v", strings.Join(ips, "\n"))
	cha := make(chan string, 10)
	go scan.PortRange(ips, uint(portMin), uint(portMax), cha)
	for {
		select {
		case h := <-cha:
			if h == "" {
				return nil
			}
			log.Info().Msg(h)
		}
	}
}
