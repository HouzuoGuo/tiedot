/* Client network IO. */
package network

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"path"
	"strconv"
	"sync"
)

// A connection to tiedot RPC server
type TiedotConn struct {
	SrvAddr, IPCSrvTmpDir string
	SrvRank               int
	In                    *bufio.Reader
	Out                   *bufio.Writer
	Conn                  *net.Conn
	Mutex                 *sync.Mutex
}

// Create a connection to a tiedot IPC server.
func NewTiedotConn(ipcSrvTmpDir string, rank int) (tc *TiedotConn, err error) {
	addr := path.Join(ipcSrvTmpDir, strconv.Itoa(rank))
	conn, err := net.Dial("unix", addr)
	if err != nil {
		return
	}
	tc = &TiedotConn{SrvAddr: addr, IPCSrvTmpDir: ipcSrvTmpDir, SrvRank: rank,
		In: bufio.NewReader(conn), Out: bufio.NewWriter(conn), Conn: &conn,
		Mutex: new(sync.Mutex)}
	return
}

// Send a request to IPC server, suffix new-line is automatically added.
func (tc *TiedotConn) writeReq(line string) {
	var err error
	tc.Mutex.Lock()
	defer tc.Mutex.Unlock()
	// The following errors may occur only if server unexpectedly closed the connection
	// Therefore the client may simply abort (panic)
	if _, err = tc.Out.WriteString(line); err != nil {
		panic(err)
	}
	if err = tc.Out.WriteByte(byte('\n')); err != nil {
		panic(err)
	}
	if err = tc.Out.Flush(); err != nil {
		panic(err)
	}
}

// Return a server response line without suffix new-line. Will wait for it if necessary.
func (tc *TiedotConn) getResp() string {
	line, err := tc.In.ReadString(byte('\n'))
	if err != nil {
		// The error occurs if server closes the connection, the client may simply abort (panic)
		panic(err)
	}
	return line[0 : len(line)-1]
}

// Send a request and expect an OK response or error.
func (tc *TiedotConn) getOK(req string) error {
	tc.writeReq(req)
	resp := tc.getResp()
	if resp != ACK {
		return errors.New(resp)
	}
	return nil
}

// Send a request and expect a uint64 response or error.
func (tc *TiedotConn) getUint(req string) (uint64, error) {
	tc.writeReq(req)
	resp := tc.getResp()
	if resp == ERR {
		return 0, errors.New(resp)
	}
	return strconv.ParseUint(resp, 10, 64)
}

// Send a request and expect a string response or error.
func (tc *TiedotConn) getStr(req string) (string, error) {
	tc.writeReq(req)
	resp := tc.getResp()
	if resp == ERR {
		return "", errors.New(resp)
	}
	return resp, nil
}

// Send a request and expect a JSON response or error.
func (tc *TiedotConn) getJSON(req string) (ret map[string]interface{}, err error) {
	strResp, err := tc.getStr(req)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(strResp), ret)
	return
}

// Close the connection. Remember to call this!
func (tc *TiedotConn) Close() {
	(*tc.Conn).Close()
}
