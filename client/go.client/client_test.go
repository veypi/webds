package client

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	"testing"
)

func TestConnection(t *testing.T) {
	log.Info().Msg("start connection")
	c := New(&Config{
		Host:             "127.0.0.1:8080",
		ID:               "123456",
		EvtMessagePrefix: nil,
		BinaryMessages:   false,
		ReadBufferSize:   0,
		WriteBufferSize:  0,
	})
	log.SetLevel(log.TraceLevel)
	c.Subscribe("test", func(data interface{}) {
		fmt.Print(data)
	})
	c.OnConnect(func() {
		c.Pub("test", "123")
	})
	c.Wait()
}
