// Binary protocol over IPC - handle message records.
package binprot

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

const (
	// Command/reply record structure
	REC_ESC    = 0   // ESC -> ESC ESC
	REC_PARAM  = 252 // PARAM -> ESC EPARAM
	REC_EPARAM = 253
	REC_END    = 254 // END -> ESC EEND
	REC_EEND   = 255
)

var recEnd []byte = []byte{REC_END}
var rEscapeRecEnd []byte = []byte{REC_ESC, REC_EEND}
var recParam []byte = []byte{REC_PARAM}
var rEscapeRecParam []byte = []byte{REC_ESC, REC_EPARAM}
var recEsc []byte = []byte{REC_ESC}
var rEscapeRecEsc []byte = []byte{REC_ESC, REC_ESC}

func Uint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func Buint64(i uint64) (buf []byte) {
	buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return
}
func Uint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}
func Buint32(i uint32) (buf []byte) {
	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	return
}
func Int32(buf []byte) int32 {
	return int32(binary.LittleEndian.Uint32(buf))
}
func Bint32(i int32) (buf []byte) {
	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(i))
	return
}

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
