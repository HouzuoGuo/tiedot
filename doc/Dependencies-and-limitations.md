## Project dependency

tiedot has only one external package dependency, which is [go.rice], used by HTTP server for serving static assets in web control panel application. You need both git and mercurial (due to go.rice dependency) to build tiedot.

If you wish to remove all (well, the only) external dependencies, simply follow the instructions in `webcp/webcp.go`. You will lose the web control panel app by doing so.

## Hardware platform limit

By default, tiedot does not compile on 32-bit systems due to:

- Hash-table key-smear algorithm overflows 32-bit integer and prevents compilation.
- Data files are not split into 2GB chunks.
- Document ID generator involves using a random number source which produces platform integer (32 or 64 bits).

However, you may safely use tiedot on 32-bit systems ONLY IF there is a very small amount of data to be managed - several thousand of documents per collection (at maximum); to do so, please follow the instructions in `buildconstraint.go`.

## Data size limit

tiedot relies on memory mapped files for almost everything - just like many other NoSQL solutions.

For best performance, it is recommended (but not required) to have enough system memory to accommodate the entire data set.

Your operating system may have additional limit on the maximum size of a single memory mapped file.

## Document size limit

A document may not exceed 2MBytes, which means:

- When inserting a new document, its size may not exceed 1MBytes.
- When updating a document, the updated version may not exceed 2MBytes.

This limit is a compile time constant, it can be easily modified in `data/collection.go` (const `DOC_MAX_ROOM`).

## Runtime and scalability limit

Upon creating a new database, all collections and indexes are partitioned into `runtime.NumCPU()` (number of system CPUs) partitions, allowing concurrent document operations to be carried out on independent partitions. See [Concurrency and networking] for more details.

Go runtime uses `GOMAXPROCS` to limit number of OS threads available to a Go program, thus it will affect the scalability of tiedot. For best performance, `GOMAXPROCS` should be set to the number of system CPUs. This can be set via tiedot CLI parameter or environment variable `GOMAXPROCS`.

[go.rice]: https://github.com/GeertJohan/go.rice
[Concurrency and networking]: https://github.com/HouzuoGuo/tiedot/wiki/Concurrency-and-networking