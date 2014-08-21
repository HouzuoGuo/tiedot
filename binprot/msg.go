package binprot

import (
	"bufio"
	"bytes"
)

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

func SrvAnsErr(out *bufio.Writer, errMsg string) (err error) {
	if err = out.WriteByte(C_ERR); err != nil {
		return
	} else if _, err = out.WriteString(errMsg); err != nil {
		return
	}
	if err = out.WriteByte(C_RS); err != nil {
		return
	}
	return out.Flush()
}

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

func ClientReadAns(in *bufio.Reader) (moreInfo [][]byte, err error) {
	status, err := in.ReadByte()
	if err != nil {
		return
	}
	reply, err := in.ReadSlice(C_RS)
	reply = reply[0 : len(reply)-1]
	if status == C_OK {
		moreInfo = bytes.Split(reply, []byte{C_US})
	} else {
		moreInfo = make([][]byte, 1)
		moreInfo[0] = reply
	}
	return
}
