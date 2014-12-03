## High level picture: ACID?

Similar to many other popular NoSQL solutions, tiedot does not provide ACID transactions. However, atomic operations are possible within the scope of a single document.

At the moment tiedot does not use a journal file, therefore it relies on operating system to periodically synchronize mapped file buffer with underlying storage device; this means, that in case of a system crash, you may lose several most recent document updates.

But tiedot data structures are extremely resilient to system crashes, making it really really difficult for any system crash to corrupt data files.

## Concurrency of document operations

In embedded usage, you are encouraged to use all public functions concurrently. However please do not use public functions in "data" package by yourself - you most likely will not need to use them directly.

When a tiedot database is created, the number of system CPUs is written down into a file called `number_of_partitions`. From there, all collections and indexes are partitioned automatically ("sharding").

These partitions function independently, to allow document operations be carried out concurrently on many partitions at once; in this way, tiedot confidently scales to 4+ CPU cores.

## Concurrency of HTTP API endpoints

You are encouraged to use all HTTP endpoints concurrently.

To ensure safe operation and data consistency, there is a very small number of HTTP endpoints which "stop the world" during their execution, these are the features operating on database schema:

- Create/rename/drop/scrub collection
- Create/remove index
- Dump/sync database

## HTTP service

tiedot HTTP service is powered by HTTP server in Go standard library `net/http`.

The HTTP service:

- Serves only one database instance
- Listens on all network interfaces
- Listens on the port specified by user via CLI parameter
- Unconditionally processes all incoming requests
- Scalability is affected by `GOMAXPROCS`

See [API reference and embedded usage] for documentation on HTTP service usage.

Once tiedot enters HTTP service mode, it keeps running in foreground until:

- `/shutdown` endpoint is called (gracefully shutdown)
- Process is stopped/interrupted/killed (not good!)

[API reference and embedded usage]: https://github.com/HouzuoGuo/tiedot/wiki/API-reference-and-embedded-usage
