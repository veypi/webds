package client

import (
	"fmt"
	"github.com/lightjiang/utils/log"
	"github.com/lightjiang/webds/message"
	"testing"
)

func TestNew(t *testing.T) {
	log.Info().Msg("start conn")
	c, err := NewFromUrl("123456", "127.0.0.1:8080", nil)
	if err != nil {
		t.Error(err)
		return
	}
	log.SetLevel(log.TraceLevel)
	c.Subscribe(message.NewTopic("test"), func(data interface{}) {
		fmt.Print(data)
	})
	c.OnConnect(func() {
		c.Publisher("test")("123")
	})
	c.Wait()
}
