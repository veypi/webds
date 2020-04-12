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
	log.Info().Msg("receive " + r.Header.Get("id"))
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
	c.LateralMaster = []string{
		"ws://127.0.0.1:8081",
		"ws://127.0.0.1:8082",
	}
	go func() {
		h := testHandler{}
		h.init(c)
		http.ListenAndServe(":8082", &h)
	}()
	h := testHandler{}
	h.init(c)
	http.ListenAndServe(":8081", &h)
}
