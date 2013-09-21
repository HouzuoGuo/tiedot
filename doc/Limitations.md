## Data size

Majority of tiedot operations are supported by memory mapped files - just like many other NoSQL solutions.

From Go runtime perspective, if the runtime is 32-bit, you may have only up to 2GB data per file; if the runtime is 64-bit, the limit becomes (2 ^ 64) bytes.

Your operating system may have additional limit on the maximum size of memory mapped file.

## Document size

Any document may not exceed 32MBytes, which means:

- By inserting a document, its size may not exceed 16MBytes.
- By updating a document, the updated version may not exceed 32MBytes.

## Runtime and scalability

Golang runtime uses `GOMAXPROCS` to determine number of OS threads available for a Go program, thus it will affect the scalability of tiedot. For best performance, `GOMAXPROCS` should be set to no less than available number of CPUs (this can be set via tiedot CLI parameter or environment variable).