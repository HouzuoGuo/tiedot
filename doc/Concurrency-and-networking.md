## High level picture: ACID?

Similar to many other popular NoSQL solutions, tiedot does not provide ACID transactions. However, atomic operations are possible within the scope of a single document.

A background goroutine is associated with every database instance, that periodically synchronizes file buffers (every 2 seconds).

There are APIs for both HTTP API service and embedded usage for manually synchronizing all buffers.

## Concurrency of IO operations

When you create a tiedot database, the number of CPUs available in the system is written down into a file called "number_of_partitions". From there, all new collections will be partitioned automatically.

Partitions function independent of each other, hence IO operations can be carried out concurrently on many partitions at once, governed by RWMutex. In this way, tiedot confidently scales to 4 CPU cores.

Indexes are also partitioned - there are as many collection partitions as there are index partitions. Governed by RWMutex, secondary index reads/updates can be carried out concurrently on many partitions.

## Concurrency of HTTP API endpoints

While most HTTP endpoints support concurrency, there is a small number of operations which must "stop the world" to ensure safe operation - these operations block __all__ other HTTP endpoints until completion:

- Create/rename/drop/scrub/repartition collection
- Create/remove index
- Dump/sync database

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