This work is based on [mmap-go][] written by [edsrzf][] using BSD-style license.

On top of the original repository, I made these bug fixes:

- Incorrect syscall parameters in Unix msync
- Panic if buffer size is too large in Windows

The modifications I made are subject to BSD-style license; for details, please read LICENSE.

[mmap-go]: https://github.com/edsrzf/mmap-go
[edsrzf]: https://github.com/edsrzf/
