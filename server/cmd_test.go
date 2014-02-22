package server

import (
	"os"
	"syscall"
	"testing"
)

func TestColMgmt(t *testing.T) {
	pid, err := syscall.ForkExec(os.Getenv("TIEDOT_EXEC"), []string{"-mode=ipc", "-myrank=0", "-totalrank=0"}, &syscall.ProcAttr{})
	if err != nil {
		panic(err)
	}
	t.Log(pid)
}
