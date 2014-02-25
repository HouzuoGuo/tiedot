/* Client network IO. */
package network

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// Send a request to IPC server, suffix new-line is automatically added.
func (tc *Client) writeReq(line string) {
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
func (tc *Client) getResp() string {
	line, err := tc.In.ReadString(byte('\n'))
	if err != nil {
		// The error occurs if server closes the connection, the client may simply abort (panic)
		panic(err)
	}
	return line[0 : len(line)-1]
}

// Send a request and expect an OK response or error.
func (tc *Client) getOK(req string) error {
	tc.writeReq(req)
	resp := tc.getResp()
	if resp != ACK {
		return errors.New(resp)
	}
	return nil
}

// Send a request and expect a uint64 response or error.
func (tc *Client) getUint64(req string) (uint64, error) {
	tc.writeReq(req)
	resp := tc.getResp()
	if strings.HasPrefix(resp, ERR) {
		return 0, errors.New(resp)
	}
	return strconv.ParseUint(resp, 10, 64)
}

// Send a request and expect a string response or error.
func (tc *Client) getStr(req string) (string, error) {
	tc.writeReq(req)
	resp := tc.getResp()
	if strings.HasPrefix(resp, ERR) {
		return "", errors.New(resp)
	}
	return resp, nil
}

// Send a request and expect a JSON response or error.
func (tc *Client) getJSON(req string) (ret interface{}, err error) {
	strResp, err := tc.getStr(req)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(strResp), &ret)
	return
}
