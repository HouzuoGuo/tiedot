/*
Test client connection AND reach out to the server using test commands.
This test must be invoked by runtd script.
*/
package network

import (
	"fmt"
	"strconv"
	"testing"
)

func TestPings(t *testing.T) {
	/*
	 Many of the following conditions are coded in runtd script:
	 - there are 4 IPC test servers
	 - servers create socket files in /tmp/tiedot_test_ipc_tmp
	*/
	for i := 0; i < 4; i++ {
		tc, err := NewClient("/tmp/tiedot_test_ipc_tmp", i)
		if err != nil {
			t.Fatal(err)
		}
		if !(tc.SrvAddr == "/tmp/tiedot_test_ipc_tmp/"+strconv.Itoa(i) && tc.SrvRank == i && tc.Conn != nil && tc.IPCSrvTmpDir == "/tmp/tiedot_test_ipc_tmp") {
			t.Fatal(tc)
		}
		if err2 := tc.getOK(PING); err2 != nil {
			t.Fatal(err)
		}
		if str, err2 := tc.getStr(PING); str != ACK || err2 != nil {
			t.Fatal(str, err2)
		}
		if i, err2 := tc.getUint64(PING1); i != 1 || err2 != nil {
			t.Fatal(i, err2)
		}
		if js, err2 := tc.getJSON(PING_JSON); js.([]interface{})[0].(string) != "OK" || err2 != nil {
			t.Fatal(i, err2)
		}
		if _, err2 := tc.getStr(PING_ERR); fmt.Sprint(err2) != ERR+"this is an error" {
			t.Fatal(err2)
		}
		tc.ShutdownServer()
	}
}
