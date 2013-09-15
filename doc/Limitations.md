## General info

Similar to other NoSQL solutions, majority of tiedot IO operations is supported by memory mapped files, therefore your operating system's limitation will apply - usually this implies a limited data file size.

Golang's definition of an `int` is "at least 32-bits" and thus it cannot map a file larger than size of `int` - depends on your Golang runtime, this is also a limit on data file size.

The above size limits apply to all hash table (indexes) and collection data (documents) files. When the size limit is to be exceeded, tiedot will panic and log a message.

## Runtime

Golang runtime uses `GOMAXPROCS` to determine number of OS threads available for a Go program, thus affecting scalability of tiedot. For best performance, `GOMAXPROCS` should be set to no less than available number of CPUs (this can be set via tiedot CLI parameter or environment variable).

## Documents specific

Any document may not exceed 32MBytes, which means:

- By inserting a document, its size may not exceed 16MBytes.
- By updating a document, the updated version may not exceed 32MBytes.