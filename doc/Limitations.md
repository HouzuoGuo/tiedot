## Data size

tiedot relies on memory mapped files for almost everything - just like many other NoSQL solutions.

For best performance, it is not recommended to have data set exceeding available system memory.

In 32-bit Go runtime environments, individual collection partition size may not exceed 2GB.

Your operating system may have additional limit on the maximum size of memory mapped file.

## Document size

Any document may not exceed 16MBytes, which means:

- By inserting a document, its size may not exceed 8MBytes.
- By updating a document, the updated version may not exceed 16MBytes.

## Runtime and scalability

Golang runtime uses `GOMAXPROCS` to determine number of OS threads available for a Go program, thus it will affect the scalability of tiedot. For best performance, `GOMAXPROCS` should be set to the maximum number of collection partitions on any collection.

This can be set via tiedot CLI parameter or environment variable.