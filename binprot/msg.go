package binprot

import (
	"bufio"
	"bytes"
)

/*

\ -> REC_ESC
\0 - REC_END
\n - REC_PARAM

String escape
\ -> \\		A -> A A
ESC -> ESC ESC

\0 -> \\0	B -> A C
END -> ESC END2

\n -> \\n	D -> A E
PARAM -> ESC PARAM2

String reverse escape
\\ -> \		A A -> A
\\0 -> \0	A C -> B
\\n -> \n	A E -> D


*/

const (
	// Command/reply record structure
	REC_ESC    = 0   // ESC -> ESC ESC
	REC_PARAM  = 252 // PARAM -> ESC EPARAM
	REC_EPARAM = 253
	REC_END    = 254 // END -> ESC EEND
	REC_EEND   = 255

	// Status reply from server
	R_OK         = 0
	R_ERR        = 1
	R_ERR_SCHEMA = 2
	R_ERR_MAINT  = 3
	R_ERR_DOWN   = 4

	// Client status - error (not a server reply)
	CLIENT_IO_ERR = 5

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

var recEnd []byte = []byte{REC_END}
var rEscapeRecEnd []byte = []byte{REC_ESC, REC_EEND}
var recParam []byte = []byte{REC_PARAM}
var rEscapeRecParam []byte = []byte{REC_ESC, REC_EPARAM}
var recEsc []byte = []byte{REC_ESC}
var rEscapeRecEsc []byte = []byte{REC_ESC, REC_ESC}

func readRec(in *bufio.Reader) (status byte, params [][]byte, err error) {
	// Read byte 0 - status
	if status, err = in.ReadByte(); err != nil {
		return
	}
	// Read till the end
	rest, err := in.ReadBytes(REC_END)
	if err != nil {
		return
	}
	rest = rest[0 : len(rest)-1]
	if len(rest) == 0 {
		params = make([][]byte, 0)
		return
	}
	params = make([][]byte, 0, 4)
	escaping := false
	param := make([]byte, 0, 8)
	for _, b := range rest {
		switch b {
		case REC_PARAM:
			params = append(params, param)
			param = make([]byte, 0, 8)
		case REC_ESC:
			if escaping {
				param = append(param, REC_ESC)
				escaping = false
			} else {
				escaping = true
			}
		case REC_EPARAM:
			if escaping {
				param = append(param, REC_PARAM)
				escaping = false
			} else {
				param = append(param, b)
			}
		case REC_EEND:
			if escaping {
				param = append(param, REC_END)
				escaping = false
			} else {
				param = append(param, b)
			}
		default:
			param = append(param, b)
		}
	}
	return
}

func writeRec(out *bufio.Writer, cmd byte, params ...[]byte) error {
	buf := make([]byte, 1, 2+len(params)*8)
	buf[0] = cmd
	for _, param := range params {
		// Escape REC_ESC
		param = bytes.Replace(param, recEsc, rEscapeRecEsc, -1)
		// Escape REC_PARAM
		param = bytes.Replace(param, recParam, rEscapeRecParam, -1)
		// Escape REC_END
		param = bytes.Replace(param, recEnd, rEscapeRecEnd, -1)
		buf = append(buf, param...)
		buf = append(buf, REC_PARAM)
	}
	buf = append(buf, REC_END)
	if _, err := out.Write(buf); err != nil {
		return err
	} else if err := out.Flush(); err != nil {
		return err
	}
	return nil
}
