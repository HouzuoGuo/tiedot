## Hardware platform

tiedot does not work with 32-bit CPUs.

## Data size

tiedot relies on memory mapped files for almost everything - just like many other NoSQL solutions.

For best performance, it is not recommended to have data set exceeding available system memory.

Your operating system may have additional limit on the maximum size of memory mapped file.

## Document size

Any document may not exceed 2MBytes, which means:

- When inserting a new document, its size may not exceed 1MBytes.
- When updating a document, the updated version may not exceed 2MBytes.

## Runtime and scalability

Golang runtime uses `GOMAXPROCS` to determine number of OS threads available for a Go program, thus it will affect the scalability of tiedot. For best performance, `GOMAXPROCS` should be set to the number of CPU cores.

This can be set via tiedot CLI parameter or environment variable `GOMAXPROCS`.