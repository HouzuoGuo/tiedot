package binprot

import (
	"bufio"
	"bytes"
)

const (
	// Command/reply record structure
	REC_ESC   = 0
	REC_PARAM = 254
	REC_END   = 255

	// Client status - error (not a server reply)
	CLIENT_IO_ERR = 255

	// Status reply from server
	R_OK         = 0
	R_ERR        = 1
	R_ERR_SCHEMA = 2
	R_ERR_MAINT  = 3
	R_ERR_DOWN   = 4

	// Document commands
	C_DOC_INSERT = 11
	C_DOC_UNLOCK = 12
	C_DOC_READ   = 13
	C_DOC_UPDATE = 14
	C_DOC_DELETE = 15

	// Index commands
	C_HT_PUT    = 21
	C_HT_GET    = 22
	C_HT_REMOVE = 23

	// Maintenance commands
	C_RELOAD      = 91
	C_SHUTDOWN    = 92
	C_PING        = 93
	C_GO_MAINT    = 95
	C_LEAVE_MAINT = 96
)

func readRec(in *bufio.Reader) (status byte, params [][]byte, err error) {
	// Read byte 0 - status
	if status, err = in.ReadByte(); err != nil {
		return
	}
	// Read reminder of the record
	rec := make([][]byte, 0, 1)
	for {
		var tillEnd []byte
		if tillEnd, err = in.ReadBytes(REC_END); err != nil {
			return
		}
		bytes.Replace(tillEnd, []byte{REC_ESC, REC_ESC}, []byte{REC_ESC}, 0)
		length := len(tillEnd)
		last := true
		// Is the record delimiter prefixed by an escape?
		switch length {
		case 1:
			rec = append(rec, tillEnd)
		case 2:
			if tillEnd[0] == REC_ESC {
				rec = append(rec, tillEnd[1:])
				last = false
			} else {
				rec = append(rec, tillEnd)
			}
		default:
			if tillEnd[length-2] == REC_ESC {
				rec = append(rec, tillEnd[0:length-2], []byte{tillEnd[length-1]})
				last = false
			} else {
				rec = append(rec, tillEnd)
			}
		}
		if last {
			break
		}
	}
	recTogether := bytes.Join(rec, []byte{})
	// Split record parameters
	params = make([][]byte, 0, 0)
	param := make([]byte, 0)
	paramPos := 0
	for i, b := range recTogether {
		if b == REC_PARAM {
			// Is the parameter delimiter prefixed by an escape?
			if recTogether[i-1] == REC_ESC {
				param = append(param, recTogether[paramPos:i-1]...)
				param = append(param, REC_PARAM)
			} else {
				param = append(param, recTogether[paramPos:i]...)
				params = append(params, param)
				param = make([]byte, 0)
			}
			paramPos = i + 1
		}
	}
	return
}

func writeRec(out *bufio.Writer, status byte, more ...[]byte) error {
	return nil
}
