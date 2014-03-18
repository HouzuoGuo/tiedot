/* Client network IO. */
package network

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// Send an unimportant message to server, do not panic on error. Return true on success.
func (tc *Client) writeAway(line string, consumeResp bool) bool {
	var err error
	if _, err = tc.Out.WriteString(line); err != nil {
		return false
	}
	if err = tc.Out.WriteByte(byte('\n')); err != nil {
		return false
	}
	if err = tc.Out.Flush(); err != nil {
		return false
	}
	if consumeResp {
		_, err = tc.In.ReadString(byte('\n'))
		return err == nil
	}
	return true
}

// Send a request to IPC server, suffix new-line is automatically added.
func (tc *Client) writeReq(line string) (err error) {
	if _, err = tc.Out.WriteString(line); err != nil {
		return
	}
	if err = tc.Out.WriteByte(byte('\n')); err != nil {
		return
	}
	if err = tc.Out.Flush(); err != nil {
		return
	}
	return
}

// Return a server response line without suffix new-line. Will wait for it if necessary.
func (tc *Client) getResp() (line string, err error) {
	line, err = tc.In.ReadString(byte('\n'))
	if err != nil {
		// The error may happen when server closes the connection
		return
	}
	return line[0 : len(line)-1], nil
}

// Send a request and expect an OK response or error.
func (tc *Client) getOK(req string) (err error) {
	if err = tc.writeReq(req); err != nil {
		return
	}
	resp, err := tc.getResp()
	if err != nil {
		return
	}
	if resp != ACK {
		return errors.New(resp)
	}
	return nil
}

// Send a request and expect a string response or error.
func (tc *Client) getStr(req string) (resp string, err error) {
	if err = tc.writeReq(req); err != nil {
		return
	}
	if resp, err = tc.getResp(); err != nil {
		return
	}
	if strings.HasPrefix(resp, ERR) {
		return "", errors.New(resp)
	}
	return resp, nil
}

// Send a request and expect a uint64 response or error.
func (tc *Client) getUint64(req string) (uint64, error) {
	intStr, err := tc.getStr(req)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(intStr, 10, 64)
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
