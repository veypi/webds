package cmd

import "github.com/urfave/cli"

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
		list,
	},
	Flags: nil,
}

var list = cli.Command{
	Name:         "list",
	ShortName:    "",
	Aliases:      nil,
	Usage:        "",
	UsageText:    "",
	Description:  "show all the topic",
	BashComplete: nil,
	Action:       runList,
}

func runList(c *cli.Context) error {
	return nil
}
