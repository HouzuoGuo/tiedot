package sharding

import (
	"bufio"
	"bytes"
	"fmt"
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

func forthAndBack(t *testing.T, status byte, params ...[]byte) {
	var stack [4096]byte
	runtime.Stack(stack[:], false)
	buffer := bytes.NewBuffer([]byte{})
	recOut := bufio.NewWriter(buffer)
	if err := writeRec(recOut, status, params...); err != nil {
		t.Fatal(err, string(stack[:]))
	}
	bufContent := buffer.Bytes()
	fmt.Println("Written\t", bufContent)
	// Calculate expected record length, including status, REC_END and escaped bytes.
	expectedLen := 2
	for _, param := range params {
		expectedLen += len(param) + 1
		expectedLen += bytes.Count(param, []byte{REC_ESC}) + bytes.Count(param, []byte{REC_PARAM}) + bytes.Count(param, []byte{REC_END})
	}
	if len(bufContent) != expectedLen {
		t.Fatal("Length mismatch", expectedLen, len(bufContent), bufContent, string(stack[:]))
	}
	// Read the record and compare against input
	statusBack, paramsBack, err := readRec(bufio.NewReader(bytes.NewReader(bufContent)))
	fmt.Println("Read\t", statusBack, paramsBack)
	if err != nil {
		t.Fatal(err, string(stack[:]))
	} else if statusBack != status {
		t.Fatal("Status mismatch", statusBack, status, string(stack[:]))
	} else if len(paramsBack) != len(params) {
		t.Fatal("Param mismatch", paramsBack, params, string(stack[:]))
	}
	for i, param := range params {
		if !cmpBytes(param, paramsBack[i]) {
			t.Fatal("Param mismatch", paramsBack[i], param, string(stack[:]))
		}
	}
}

func TestRecReadWrite(t *testing.T) {
	forthAndBack(t, R_OK)
	forthAndBack(t, R_ERR)
	forthAndBack(t, R_ERR_DOWN)
	forthAndBack(t, R_ERR_MAINT)
	forthAndBack(t, R_ERR_SCHEMA)
	forthAndBack(t, R_OK, []byte{1})
	forthAndBack(t, R_ERR, []byte{1})
	forthAndBack(t, R_OK, []byte{1}, []byte{2, 3})
	forthAndBack(t, R_ERR, []byte{1}, []byte{2, 3})
	forthAndBack(t, R_OK, []byte{1}, []byte{2, 3}, []byte{4, 5, 6})
	forthAndBack(t, R_ERR, []byte{1}, []byte{2, 3}, []byte{4, 5, 6})

	forthAndBack(t, R_OK, []byte{REC_ESC})
	forthAndBack(t, R_ERR, []byte{REC_PARAM})
	forthAndBack(t, R_ERR_DOWN, []byte{REC_END})
	forthAndBack(t, R_ERR_MAINT, []byte{REC_ESC, REC_EPARAM})
	forthAndBack(t, R_ERR_SCHEMA, []byte{REC_ESC, REC_PARAM, REC_END, REC_ESC, REC_EPARAM, REC_EEND})
	forthAndBack(t, R_OK, []byte{REC_EEND, REC_EPARAM, REC_ESC, REC_END, REC_PARAM, REC_ESC})

	forthAndBack(t, R_OK, []byte{1, REC_PARAM, 2, REC_END, 3, REC_ESC})
	forthAndBack(t, R_OK, []byte{REC_ESC, 1, REC_EPARAM, 2, REC_END, 3})
}
