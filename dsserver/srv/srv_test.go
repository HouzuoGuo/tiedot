package srv

import (
	"os"
	"testing"
	"sync"
)

func TestNewServerNoOpen(t *testing.T) {
	wds := []string{"/tmp/tiedot_srv1", "/tmp/tiedot_srv2", "/tmp/tiedot_srv3", "/tmp/tiedot_srv4"}
	for _, wd := range wds {
		os.RemoveAll(wd)
	}
	os.RemoveAll("/tmp/tiedot_db")
	srvs := make([]*Server, 4)
	ready := &sync.WaitGroup{}
	ready.Add(4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			var err error
			srvs[i], err = NewServer(i, 4, "/tmp/tiedot_db", wds[i])
			if err != nil {
				t.Fatal(err)
			}
			ready.Done()
		}(i)
	}
	ready.Wait()
	for i := 0; i < 4; i++ {
		srv := srvs[i]
		if !(srv.Rank == i && srv.TotalRank == 4 && srv.WorkingDir == wds[i] && srv.DBDir == "/tmp/tiedotdb" && srv.Barrier == false &&
			len(srv.ColNumParts) == 0 && len(srv.ColParts) == 0 && len(srv.Htables) == 0 && len(srv.MainLoop) == 0 &&
			len(srv.InterRank) == 4 && srv.Listener != nil) {
			t.Fatal(srv)
		}
	}
}
