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
	c.LateralMaster = []string{
		"ws://127.0.0.1:8081",
		"ws://127.0.0.1:8082",
		"ws://127.0.0.1:8083",
	}
	newC := func(c Config) {
		h := testHandler{}
		h.init(c)
		http.ListenAndServe(":"+c.ID, &h)
	}
	c.ID = "8081"
	go newC(c)
	c.ID = "8082"
	newC(c)
	//c.ID = "8083"
	//newC(c)
}
