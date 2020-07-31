package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/veypi/utils/log"
	"github.com/veypi/webds/libs"
)

var ports = cli.NewIntSlice(10000, 10100)

var Scan = &cli.Command{
	Name:      "scan",
	ArgsUsage: "scan 192.168.0.1/24 10.0.0.0/24 192.168.1.1/24 --ports='8000 9000'.",
	Flags: []cli.Flag{
		&cli.IntSliceFlag{
			Name:  "ports",
			Usage: "ports range that you want to scan",
			Value: ports,
		},
	},
	Action: runScan,
}

func runScan(c *cli.Context) error {
	ports := c.IntSlice("ports")
	if len(ports) != 2 {
		log.Warn().Msg("please add ports range")
		return nil
	}
	ips := c.Args().Slice()
	if len(ips) == 0 {
		ips = append(libs.GetLocalIps(), "127.0.0.1/32")
	}
	//ips = []string{"127.0.0.1/32"}
	for _, k := range ips {
		scanner, err := libs.NewScanner(k)
		if err != nil {
			log.Warn().Msg(err.Error())
			continue
		}
		r := scanner.ScanPortRange(uint(ports[0]), uint(ports[1]))
		//r := scanner.Scan()
		if len(r) > 0 {
			fmt.Println(r)
		}
	}
	return nil
}
