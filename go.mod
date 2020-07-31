module github.com/veypi/webds

go 1.13

require (
	github.com/golang/protobuf v1.4.1
	github.com/json-iterator/go v1.1.9
	github.com/rs/xid v1.2.1
	github.com/sparrc/go-ping v0.0.0-20190613174326-4e5b6552494c
	github.com/urfave/cli/v2 v2.2.0
	github.com/veypi/utils v0.1.5
	google.golang.org/protobuf v1.25.0
	nhooyr.io/websocket v1.7.4
)

replace github.com/veypi/utils v0.1.5 => ../utils
