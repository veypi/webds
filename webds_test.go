package webds

import (
	"github.com/lightjiang/utils/log"
	"net/http"
	"testing"
)

type testHandler struct {
	upgrade *Server
}

func (t *testHandler) init(c Config) {
	t.upgrade = New(c)
}

func (t *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info().Msgf("%s: receive %s", t.upgrade.ID(), r.Header.Get("id"))
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
	ch := make(chan bool, 1)
	c := Config{}
	c.IDGenerator = func(r *http.Request) string {
		return r.Header.Get("id") + "1"
	}
	c.ID = "8081"
	c.LateralMaster = []string{
		"ws://127.0.0.1:8081",
		"ws://127.0.0.1:8082",
	}
	go func(c Config) {
		h := testHandler{}
		h.init(c)
		ch <- true
		http.ListenAndServe(":8081", &h)
	}(c)
	<-ch
	h := testHandler{}
	c.ID = "8082"
	h.init(c)
	http.ListenAndServe(":8082", &h)
}
