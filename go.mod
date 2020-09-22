module github.com/veypi/webds

go 1.13

require (
	github.com/fatih/color v1.9.0
	github.com/golang/protobuf v1.4.2
	github.com/google/gopacket v1.1.18
	github.com/json-iterator/go v1.1.9
	github.com/rs/xid v1.2.1
	github.com/sparrc/go-ping v0.0.0-20190613174326-4e5b6552494c
	github.com/urfave/cli/v2 v2.2.0
	github.com/veypi/utils v0.1.5
	google.golang.org/protobuf v1.25.0
	nhooyr.io/websocket v1.8.6
)

replace github.com/veypi/utils v0.1.5 => ../utils

replace github.com/sparrc/go-ping v0.0.0-20190613174326-4e5b6552494c => github.com/veypi/go-ping v0.0.0-20200731103801-5e8a61eae67d
