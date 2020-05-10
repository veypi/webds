package webds

import (
	"github.com/lightjiang/utils/log"
	"math/rand"
	"net/http"
	"testing"
	"time"
)

type testHandler struct {
	upgrade *Server
}

func (t *testHandler) init(c Config) {
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
		h.init(c)
		time.Sleep(time.Millisecond * time.Duration(seed.Int31n(1000)))
		http.ListenAndServe(":"+c.ID, &h)
	}
	c.ID = "8081"
	c.LateralMaster = LateralMaster[:2]
	go newC(c)
	c.ID = "8082"
	c.LateralMaster = LateralMaster[:2]
	go newC(c)
	c.ID = "8083"
	c.LateralMaster = LateralMaster[2:]
	go newC(c)
	c.LateralMaster = LateralMaster[1:]
	c.ID = "8084"
	time.Sleep(time.Second * 3)
	newC(c)
}
