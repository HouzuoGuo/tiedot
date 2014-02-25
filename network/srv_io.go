/* Server network IO. */
package network

import (
	"bufio"
	"encoding/json"
	"fmt"
)

// Write a line to the output buffer, automatically add suffix new-line.
func (srv *Server) writeLnToOut(line string, out *bufio.Writer) (err error) {
	// These errors happen when client closes their connection
	if _, err = out.WriteString(line); err != nil {
		return
	}
	if err = out.WriteByte(byte('\n')); err != nil {
		return
	}
	return out.Flush()
}

// Submit a task to server, upon task completion, write an ACK or task error to the output.
func (srv *Server) ackOrErr(task *Task, out *bufio.Writer) (err error) {
	switch resp := srv.Submit(task).(type) {
	case error:
		return srv.writeLnToOut(fmt.Sprint(ERR, resp), out)
	default:
		return srv.writeLnToOut(ACK, out)
	}
}

// Submit a task to server, upon task completion, write task result string or error to the output.
func (srv *Server) strOrErr(task *Task, out *bufio.Writer) (err error) {
	switch resp := srv.Submit(task).(type) {
	case string:
		return srv.writeLnToOut(resp, out)
	default: // error
		return srv.writeLnToOut(fmt.Sprint(ERR, resp), out)
	}
}

// Submit a task to server, upon task completion, write task result uint64 or error to the output.
func (srv *Server) uint64OrErr(task *Task, out *bufio.Writer) (err error) {
	switch resp := srv.Submit(task).(type) {
	case uint64:
		return srv.writeLnToOut(fmt.Sprint(resp), out)
	default: // error
		return srv.writeLnToOut(fmt.Sprint(ERR, resp), out)
	}
}

// Submit a task to server, upon task completion, write task result JSON string or error to the output.
func (srv *Server) jsonOrErr(task *Task, out *bufio.Writer) (err error) {
	switch resp := srv.Submit(task).(type) {
	case error:
		return srv.writeLnToOut(fmt.Sprint(ERR, resp), out)
	default:
		js, err2 := json.Marshal(resp)
		if err2 != nil {
			return srv.writeLnToOut(fmt.Sprint(ERR, resp), out)
		}
		return srv.writeLnToOut(string(js), out)
	}
}
