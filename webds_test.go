package webds

import (
	"github.com/veypi/utils/log"
	"math/rand"
	"net/http"
	"testing"
	"time"
)

type testHandler struct {
	upgrade *webds
}

func (t *testHandler) init(c *Config) {
	t.upgrade = New(c)
}

func (t *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := t.upgrade.Upgrade(w, r)
	if err != nil {
		log.HandlerErrs(err)
		return
	}
	conn.Wait()
}

func TestNew(t *testing.T) {
	log.SetLevel(log.TraceLevel)
	log.Info().Msg("start webds server")
	c := Config{}
	c.EnableCluster = true
	c.IDGenerator = func(r *http.Request) string {
		return r.Header.Get("id")
	}
	var seed = rand.New(rand.NewSource(time.Now().UnixNano()))
	LateralMaster := []string{
		"ws://127.0.0.1:8081",
		"ws://127.0.0.1:8082",
		"ws://127.0.0.1:8083",
		"ws://127.0.0.1:8084",
	}
	newC := func(c Config) {
		h := testHandler{}
		time.Sleep(time.Millisecond * time.Duration(seed.Int31n(1000)))
		h.init(&c)
		log.HandlerErrs(http.ListenAndServe(":"+c.ID, &h))
	}
	c.ID = "8081"
	c.LateralMaster = LateralMaster[:3]
	go newC(c)
	c.ID = "8082"
	c.LateralMaster = LateralMaster[:1]
	go newC(c)
	c.ID = "8083"
	c.SuperiorMaster = LateralMaster[3:]
	go newC(c)
	//c.LateralMaster = LateralMaster[1:]
	c.LateralMaster = nil
	c.SuperiorMaster = nil
	c.ID = "8084"
	time.Sleep(time.Second * time.Duration(seed.Int31n(5)))
	newC(c)
}
