// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin freebsd linux netbsd openbsd

package gommap

import (
	"log"
	"syscall"
)

func mmap(len int, fd uintptr) ([]byte, error) {
	log.Println("File has been mapped", fd, len)
	return syscall.Mmap(int(fd), 0, len, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func unmap(addr, len uintptr) error {
	log.Println("File has been unmapped", addr, len)
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, addr, len, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}
