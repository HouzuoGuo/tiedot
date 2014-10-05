package binprot

import (
	"bufio"
	"bytes"
	"testing"
	"runtime"
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

func TestReadRec(t *testing.T) {
	readAndExpect(t, []byte{R_OK, REC_END}, R_OK, 0)
	readAndExpect(t, []byte{R_ERR_MAINT, REC_END}, R_ERR_MAINT, 0)
	readAndExpect(t, []byte{R_OK,
		1, REC_PARAM,
		2, REC_PARAM,
		REC_END},
		R_OK,
		2, []byte{1}, []byte{2})
	readAndExpect(t, []byte{R_OK,
		REC_ESC, REC_PARAM, REC_PARAM,
		REC_ESC, REC_PARAM, REC_PARAM,
		REC_END},
		R_OK,
		2, []byte{REC_PARAM}, []byte{REC_PARAM})
	readAndExpect(t, []byte{R_OK,
		1, 2, REC_ESC, REC_PARAM, 3, REC_PARAM,
		4, 5, REC_ESC, REC_PARAM, 6, REC_PARAM,
		REC_END},
		R_OK,
		2, []byte{1, 2, REC_PARAM, 3}, []byte{4, 5, REC_PARAM, 6})
	readAndExpect(t, []byte{R_ERR_DOWN,
		1, 2, REC_ESC, REC_END, 3, 4, REC_ESC, REC_PARAM, 5, REC_PARAM,
		REC_END},
		R_ERR_DOWN,
		1, []byte{1, 2, REC_END, 3, 4, REC_PARAM, 5})
	readAndExpect(t, []byte{R_ERR_DOWN,
		1, 2, REC_ESC, REC_PARAM, 3, REC_PARAM,
		4, REC_ESC, REC_END, 5, REC_PARAM,
		REC_END},
		R_ERR_DOWN,
		2, []byte{1, 2, REC_PARAM, 3}, []byte{4, REC_END, 5})
}
