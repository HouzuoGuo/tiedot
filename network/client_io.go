/* Client network IO. */
package network

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// Send a request to IPC server, suffix new-line is automatically added.
func (tc *Client) writeReq(rank int, line string) (err error) {
	if _, err = tc.Out[rank].WriteString(line); err != nil {
		return
	}
	if err = tc.Out[rank].WriteByte(byte('\n')); err != nil {
		return
	}
	if err = tc.Out[rank].Flush(); err != nil {
		return
	}
	return
}

// Return a server response line without suffix new-line. Will wait for it if necessary.
func (tc *Client) getResp(rank int) (line string, err error) {
	line, err = tc.In[rank].ReadString(byte('\n'))
	if err != nil {
		// The error may happen when server closes the connection
		return
	}
	return line[0 : len(line)-1], nil
}

// Send a request and expect an OK response or error.
func (tc *Client) getOK(rank int, req string) (err error) {
	if err = tc.writeReq(rank, req); err != nil {
		return
	}
	resp, err := tc.getResp(rank)
	if err != nil {
		return
	}
	if resp != ACK {
		return errors.New(resp)
	}
	return nil
}

// Send a request and expect a string response or error.
func (tc *Client) getStr(rank int, req string) (resp string, err error) {
	if err = tc.writeReq(rank, req); err != nil {
		return
	}
	if resp, err = tc.getResp(rank); err != nil {
		return
	}
	if strings.HasPrefix(resp, ERR) {
		return "", errors.New(resp)
	}
	return resp, nil
}

// Send a request and expect a uint64 response or error.
func (tc *Client) getUint64(rank int, req string) (uint64, error) {
	intStr, err := tc.getStr(rank, req)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(intStr, 10, 64)
}

// Send a request and expect a JSON response or error.
func (tc *Client) getJSON(rank int, req string) (ret interface{}, err error) {
	strResp, err := tc.getStr(rank, req)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(strResp), &ret)
	return
}
