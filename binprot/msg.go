// Message handling utility functions.

package binprot

import (
	"bufio"
	"bytes"
)

// Server reads a "CMD-PARAM-US-PARAM-RS" command sent by client.
func SrvReadCmd(in *bufio.Reader) (cmd byte, params [][]byte, err error) {
	if cmd, err = in.ReadByte(); err != nil {
		return
	}
	record, err := in.ReadSlice(C_RS)
	record = record[0 : len(record)-1]
	if err != nil {
		return
	}
	params = bytes.Split(record, []byte{C_US})
	return
}

// Server answers "OK-INFO-US-INFO-RS".
func SrvAnsOK(out *bufio.Writer, moreInfo ...[]byte) (err error) {
	if err = out.WriteByte(C_OK); err != nil {
		return
	}
	for _, more := range moreInfo {
		if _, err = out.Write(more); err != nil {
			return
		} else if err = out.WriteByte(C_US); err != nil {
			return
		}
	}
	if err = out.WriteByte(C_RS); err != nil {
		return
	}
	return out.Flush()
}

// Server answers "ERR-MSG-MSG-RS".
func SrvAnsErr(out *bufio.Writer, errCode byte, errMsg ...string) (err error) {
	if err = out.WriteByte(errCode); err != nil {
		return
	}
	for _, msg := range errMsg {
		if _, err = out.WriteString(msg); err != nil {
			return
		}
	}
	if err = out.WriteByte(C_RS); err != nil {
		return
	}
	return out.Flush()
}

// Client sends a "CMD-PARAM-US-PARAM-RS" command to server.
func ClientWriteCmd(out *bufio.Writer, cmd byte, params ...[]byte) (err error) {
	if err = out.WriteByte(cmd); err != nil {
		return
	}
	for _, param := range params {
		if _, err = out.Write(param); err != nil {
			return
		} else if err = out.WriteByte(C_US); err != nil {
			return
		}
	}
	if err = out.WriteByte(C_RS); err != nil {
		return
	}
	return out.Flush()
}

// Client reads server's response.
func ClientReadAns(in *bufio.Reader) (moreInfo [][]byte, errCode byte, err error) {
	status, err := in.ReadByte()
	if err != nil {
		return
	}
	reply, err := in.ReadSlice(C_RS)
	reply = reply[0 : len(reply)-1]
	if status == C_OK {
		moreInfo = bytes.Split(reply, []byte{C_US})
	} else {
		errCode = status
		moreInfo = make([][]byte, 1)
		moreInfo[0] = reply
	}
	return
}
