## High level picture: ACID?

Similar to many other popular NoSQL solutions, tiedot does not provide ACID transactions. However, atomic operations are possible within the scope of a single document.

Buffer synchronization ("durability") is entirely automated when tiedot runs HTTP services, however embedded tiedot instance must manually invoke buffer synchronization APIs.

## Concurrency of IO operations

tiedot collection is partitioned into number of chunks; partitions function independent of each other, hence IO operations can be carried out concurrently on many partitions at once, governed by RWMutex.

Secondary indexes are also partitioned - there are as many collection partitions as there are sec.index partitions. Governed by RWMutex, secondary index reads/updates can be carried out concurrently on many partitions.

## Concurrency of HTTP API endpoints.

While most HTTP endpoints support concurrency, there is a small number of operations which must "stop the world" to ensure safe operation - these operations block __all__ other HTTP endpoints until completed:

- Create/rename/drop/scrub/repartition collection
- Create/remove index
- Dump/flush database

Governed by a RWMutex, "stop the world" operations put write lock and all non-blocking operations put read lock on it.

## HTTP service

tiedot HTTP service is powered by HTTP server in standard Golang library `net/http`.

The HTTP service:

- Serves only one database instance
- Listens on all network interfaces
- Listens on the port specified by user via CLI parameter
- Unconditionally processes all incoming requests
- Scalability is affected by `GOMAXPROCS`

See [API reference and embedded usage] for documentation HTTP service usage.

Once tiedot enters HTTP service mode, it keeps running in foreground until:

- `/shutdown` endpoint is called (gracefully shutdown)
- Process is stopped/interrupted/killed (not good!)

[API reference and embedded usage]: https://github.com/HouzuoGuo/tiedot/wiki/API-reference-and-embedded-usage