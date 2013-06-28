# Limitations

## General info

Similar to other NoSQL solutions, majority of tiedot IO operations is carried out on memory mapped files, thus general limitations of your operating platform will apply - usually this implies a limited data file size.

Golang's definition of an `int` is "at least 32-bits" and thus it cannot map a file larger than size of `int` - depends on your Golang runtime, this is also a limit on data file size.

The above size limits apply to all hash table (indexes) and collection data (documents) files.

## Runtime

Golang runtime uses `GOMAXPROCS` to determine number of OS threads available for a Go program, thus affecting scalability of tiedot.

## Documents specific

Any document may not exceed 32MBytes, which means:

- By inserting a document, its size may not exceed 16MBytes.
- By updating a document, the updated version may not exceed 32MBytes.
