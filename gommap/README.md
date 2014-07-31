This work is based on [mmap-go][] (BSD-style license) written by [Evan Shaw][].

On top of the original work, I further simplified the API usage, and fixed two minor bugs:

- Fix incorrect parameter usage in msync syscall (Unix)
- Will panic if the desired memory buffer size is too large (Windows)

[mmap-go]: https://github.com/edsrzf/mmap-go
[Evan Shaw]: https://github.com/edsrzf/
