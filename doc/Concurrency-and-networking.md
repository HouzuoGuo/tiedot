## High level picture: ACID?

Similar to many other popular NoSQL solutions, tiedot does not provide ACID transactions. However, atomic operations are possible within the scope of a single document.

Buffer synchronization ("durability") is entirely automated when tiedot runs HTTP services, however embedded tiedot instance must manually invoke buffer synchronization APIs.

## IO operation synchronization

For maximum performance and scalability, tiedot synchronizes IO operations at very low level - each data file (document data or index) is divided into regions, and each region has a RW mutex to control concurrent access.

There are only 4 operations which incur read lock:

- Read document ( __without__ JSON parsing)
- Scan collection ( __without__ JSON parsing)
- Scan hash table (all entries)
- Scan hash table (by hash key)

And only 5 operations incur write lock:

- Insert/update/delete document ( __without__ JSON parsing)
- Put/remove hash entry

## HTTP endpoint synchronization

Most of HTTP endpoints never lock, however there is a small number of operations which must "stop the world" to ensure safe operation - these operations block __all__ other HTTP endpoints until completed:

- Create/rename/drop/scrub collection
- Create/remove index
- Dump database

The synchronization behaviour is controlled by a `RWMutex` - "stop the world" operations put write lock on it, all other operations put read lock on it.

## HTTP service

tiedot HTTP service is powered by HTTP server in standard Golang library `net/http`.

The HTTP service:

- Serves only one database instance
- Listens on all network interfaces
- Listens on the port specified by user via CLI parameter
- Unconditionally processes all incoming requests
- Scalability is affected by `GOMAXPROCS`

See "HTTP API Reference" for documentation HTTP service usage.

Once tiedot enters HTTP service mode, it keeps running in foreground until:

- `/shutdown` endpoint is called (gracefully shutdown)
- Process is stopped/interrupted/killed (not good!)