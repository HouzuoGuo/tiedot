package binprot

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"path"
	"strconv"
)

type BinProtClient struct {
	rank                int
	workspace, sockPath string
	sock                net.Conn
	in                  *bufio.Reader
	out                 *bufio.Writer
}

func NewClient(rank int, workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		rank:      rank,
		workspace: workspace,
		sockPath:  path.Join(workspace, strconv.Itoa(rank), SOCK_FILE)}
	if client.sock, err = net.Dial("unix", client.sockPath); err != nil {
		return
	}
	client.in = bufio.NewReader(client.sock)
	client.out = bufio.NewWriter(client.sock)
	return
}

func (client *BinProtClient) Ping() (err error) {
	if err = ClientWriteCmd(client.out, C_PING); err != nil {
		return
	}
	_, err = ClientReadAns(client.in)
	return
}

func (client *BinProtClient) PingErr() (err error) {
	if err = ClientWriteCmd(client.out, C_PING_ERR); err != nil {
		return
	}
	if msg, err := ClientReadAns(client.in); err != nil || string(msg[0]) != "this is an error" {
		return fmt.Errorf("IO error or unexpected response: %v %v %v", msg[0], err, []byte("this is an error"))
	}
	return
}

func (client *BinProtClient) Shutdown() {
	if err := client.sock.Close(); err != nil {
		tdlog.Noticef("Failed to close client socket: %v", err)
	}
}
