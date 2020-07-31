package webds

import (
	"github.com/veypi/utils/log"
	"math/rand"
	"net/http"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	log.SetLevel(log.TraceLevel)
	log.Info().Msg("start webds server")
	c := Config{
		EnableCluster:    true,
		EnableAutoDetect: true,
		ClusterMode:      0,
		ClusterLevel:     2,
	}
	c.IDGenerator = func(r *http.Request) string {
		return "user_" + r.Header.Get("id")
	}
	var seed = rand.New(rand.NewSource(time.Now().UnixNano()))
	newC := func(c Config) {
		w := New(&c)
		time.Sleep(time.Millisecond * time.Duration(seed.Int31n(1000)))
		err := w.AutoListen()
		if err != nil {
			log.Warn().Msg(err.Error())
		}
	}
	c.ID = "n1"
	//c.ClusterMasters = master[:3]
	go newC(c)
	c.ID = "n2"
	go newC(c)
	c.ID = "n3"
	go newC(c)
	c.ID = "n4"
	c.ClusterLevel = 1
	time.Sleep(time.Second * time.Duration(seed.Int31n(5)+4))
	newC(c)
}
