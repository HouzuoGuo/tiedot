package sharding

import (
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
)

// Return path to my executable (tiedot program).
func getMyExecutablePath() string {
	path, err := os.Readlink("/proc/self/exe")
	if err != nil {
		tdlog.Panicf("IPC Supervisor: cannot get my executable's path, are you running Linux? - %v", err)
	}
	return path
}

// Serve a database by running a number of IPC server processes and supervise them.
func RunIPCServerSupervisor(dbdir string) {
	// Initialise DB directory if it has not been done yet
	if err := data.DBNewDir(dbdir, runtime.GOMAXPROCS(0)); err != nil {
		tdlog.Panicf("IPC Supervisor: cannot initialise database directory %s - %v", dbdir, err)
	}
	dbfs, err := data.DBReadDir(dbdir)
	if err != nil {
		panic(err)
	}
	// Prepare server processes
	procs := make([]*exec.Cmd, dbfs.NShards)
	for i := 0; i < dbfs.NShards; i++ {
		newproc := exec.Command(getMyExecutablePath(),
			"-mode=ipc-server-process",
			"-ipcdbdir="+dbdir,
			"-ipcserverrank="+strconv.Itoa(i),
			"-gomaxprocs=1")
		newproc.Stdout = os.Stdout
		newproc.Stderr = os.Stderr
		procs[i] = newproc
	}
	// Run server processes
	abnormalExit := make(chan int, dbfs.NShards)
	for i, proc := range procs {
		if err := proc.Start(); err != nil {
			tdlog.Panicf("IPC Supervisor: failed to start server process - %v", err)
		}
		go func(i int, proc *exec.Cmd) {
			if err := proc.Wait(); err != nil {
				abnormalExit <- i
			}
		}(i, proc)
	}
	// If any process dies, kill the others too.
	select {
	case procNum := <-abnormalExit:
		tdlog.Noticef("IPC Supervisor: server process %d has failed, killing all others.", procNum)
		for i := 0; i < dbfs.NShards; i++ {
			if i == procNum {
				continue
			}
			if err := procs[i].Process.Kill(); err != nil {
				tdlog.Notice("IPC Supervisor: failed to kill server process %d - %v", procNum, err)
			}
		}
	}
}
