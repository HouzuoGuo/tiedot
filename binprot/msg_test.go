package binprot

import (
	"bufio"
	"bytes"
	"runtime"
	"testing"
)

func cmpBytes(one []byte, two []byte) bool {
	if len(one) != len(two) {
		return false
	}
	for i, b := range one {
		if b != two[i] {
			return false
		}
	}
	return true
}

func readAndExpect(t *testing.T, buf []byte, expectStatus byte, expectLenParams int, expectParams ...[]byte) {
	var stack [4096]byte
	runtime.Stack(stack[:], false)
	status, params, err := readRec(bufio.NewReader(bytes.NewReader(buf)))
	if err != nil {
		t.Fatal(err)
	} else if status != expectStatus {
		t.Fatal("Status mismatch", status, expectStatus, "\n", string(stack[:]))
	} else if len(params) != expectLenParams {
		t.Fatal("LenParams mismatch", len(params), expectLenParams, "\n", string(stack[:]))
	}
	for i, param := range params {
		if !cmpBytes(param, expectParams[i]) {
			t.Fatal("Params at", i, param, "does not match", expectParams[i], "\n", string(stack[:]))
		}
	}
}

func TestReadRecEscSpecial(t *testing.T) {
	// No params
	readAndExpect(t, []byte{R_OK, REC_END}, R_OK, 0)
	readAndExpect(t, []byte{R_ERR_MAINT, REC_END}, R_ERR_MAINT, 0)
	// 1 param no escape
	readAndExpect(t, []byte{
		R_ERR_DOWN,
		1, 2, 3, REC_PARAM,
		REC_END},
		R_ERR_DOWN,
		1,
		[]byte{1, 2, 3})
	// 1 param with escape
	readAndExpect(t, []byte{
		R_ERR_DOWN,
		1, 2, REC_ESC, REC_END, 3, 4, REC_ESC, REC_PARAM, 5, REC_PARAM,
		REC_END},
		R_ERR_DOWN,
		1,
		[]byte{1, 2, REC_END, 3, 4, REC_PARAM, 5})
	// 2 params no escape
	readAndExpect(t, []byte{
		R_OK,
		1, REC_PARAM,
		2, REC_PARAM,
		REC_END},
		R_OK,
		2,
		[]byte{1},
		[]byte{2})
	// 2 params escape only
	readAndExpect(t, []byte{
		R_OK,
		REC_ESC, REC_PARAM, REC_PARAM,
		REC_ESC, REC_PARAM, REC_PARAM,
		REC_END},
		R_OK,
		2,
		[]byte{REC_PARAM},
		[]byte{REC_PARAM})
	// 2 params escape REC_PARAM
	readAndExpect(t, []byte{
		R_OK,
		1, 2, REC_ESC, REC_PARAM, 3, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, 6, REC_PARAM,
		REC_END},
		R_OK,
		2,
		[]byte{1, 2, REC_PARAM, 3},
		[]byte{4, 5, REC_PARAM, 6})
	// 2 params escape REC_END
	readAndExpect(t, []byte{
		R_ERR_DOWN,
		1, 2, REC_ESC, REC_PARAM, 3, REC_PARAM,
		4, REC_ESC, REC_END, 5, REC_PARAM,
		REC_END},
		R_ERR_DOWN,
		2,
		[]byte{1, 2, REC_PARAM, 3},
		[]byte{4, REC_END, 5})
	// 3 params escape REC_PARAM
	readAndExpect(t, []byte{
		R_OK,
		1, 2, REC_ESC, REC_PARAM, 3, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, 6, REC_PARAM,
		7, 8, 9, REC_PARAM,
		REC_END},
		R_OK,
		3,
		[]byte{1, 2, REC_PARAM, 3},
		[]byte{4, 5, REC_PARAM, 6},
		[]byte{7, 8, 9})
	// 3 params escape REC_END and REC_PARAM
	readAndExpect(t, []byte{
		R_OK,
		1, 2, REC_ESC, REC_END, 3, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, 6, REC_PARAM,
		7, 8, 9, REC_PARAM,
		REC_END},
		R_OK,
		3,
		[]byte{1, 2, REC_END, 3},
		[]byte{4, 5, REC_PARAM, 6},
		[]byte{7, 8, 9})
	// 3 params, two escape in a row, escape REC_END and REC_PARAM
	readAndExpect(t, []byte{
		R_OK,
		1, 2, REC_ESC, REC_END, REC_ESC, REC_END, 3, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, REC_ESC, REC_PARAM, 6, REC_PARAM,
		7, 8, 9, REC_PARAM,
		REC_END},
		R_OK,
		3,
		[]byte{1, 2, REC_END, REC_END, 3},
		[]byte{4, 5, REC_PARAM, REC_PARAM, 6},
		[]byte{7, 8, 9})
	// 3 params, two escape in a row, escape REC_END and REC_PARAM
	readAndExpect(t, []byte{
		R_OK,
		1, 2, REC_ESC, REC_END, 3, REC_ESC, REC_END, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, 6, REC_ESC, REC_PARAM, REC_PARAM,
		7, 8, 9, REC_PARAM,
		REC_END},
		R_OK,
		3,
		[]byte{1, 2, REC_END, 3, REC_END},
		[]byte{4, 5, REC_PARAM, 6, REC_PARAM},
		[]byte{7, 8, 9})
}

func TestReadRecEscEsc(t *testing.T) {
	return
	// 1 param
	readAndExpect(t, []byte{
		R_OK,
		REC_ESC, REC_ESC, REC_PARAM,
		REC_END},
		R_OK,
		1,
		[]byte{REC_ESC})
	readAndExpect(t, []byte{
		R_ERR_MAINT,
		REC_ESC, REC_ESC, REC_ESC, REC_PARAM, REC_PARAM,
		REC_END},
		R_ERR_MAINT,
		1,
		[]byte{REC_ESC, REC_PARAM})
	return

}
