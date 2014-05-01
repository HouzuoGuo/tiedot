package dataclient

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/rpc"
	"path"
	"strconv"
	"strings"
)

type Client struct {
	srvWorkingDir string
	totalRank     int
	srvs          []*rpc.Client
}

// Create a new Client, connect to all server ranks.
func NewClient(totalRank int, srvWorkingDir string) (client *Client, err error) {
	client = &Client{srvWorkingDir, totalRank, make([]*rpc.Client, totalRank)}
	for i := 0; i < totalRank; i++ {
		if client.srvs[i], err = rpc.Dial("unix", path.Join(srvWorkingDir, strconv.Itoa(i))); err != nil {
			return
		}
	}
	return
}

// Shutdown all servers.
func (client *Client) Shutdown() (err error) {
	discard := new(bool)
	errs := make([]string, 0, 1)
	for i, srv := range client.srvs {
		if err := srv.Call("DataSvc.Shutdown", false, discard); err == nil || !strings.Contains(fmt.Sprint(err), "unexpected EOF") {
			errs = append(errs, fmt.Sprintf("Could not shutdown server rank %d", i))
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
		tdlog.Errorf("Shutdown did not fully complete, but best effort has been made: %v", err)
	}
	return
}
