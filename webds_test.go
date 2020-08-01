package webds

import (
	"fmt"
	"github.com/veypi/utils/log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	log.SetLevel(log.TraceLevel)
	c := Config{
		EnableCluster:    true,
		EnableAutoDetect: true,
		ClusterMode:      0,
		ClusterLevel:     3,
	}

	log.Info().Msgf("start webds server ")
	c.IDGenerator = func(r *http.Request) string {
		return "c" + r.Header.Get("id")
	}
	var seed = rand.New(rand.NewSource(time.Now().UnixNano()))
	newC := func(c Config) {
		w := New(&c)
		//time.Sleep(time.Millisecond * time.Duration(seed.Int31n(1000)))
		err := w.AutoListen()
		if err != nil {
			log.Warn().Msg(err.Error())
		}
	}
	for i := 1; i < 6; i++ {
		c.ID = fmt.Sprintf("n%d", i)
		go newC(c)
	}
	c.ID = "n0"
	c.ClusterLevel = 2
	go newC(c)
	time.Sleep(time.Second * time.Duration(seed.Int31n(5)+1))
	http.ListenAndServe(":8000", nil)
}
