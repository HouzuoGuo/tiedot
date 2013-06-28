# Concurrency and networking

## IO operation synchronization

For maximum performance and scalability, tiedot only synchronizes IO operations at very low level - each data file (documents or index) has a `RWMutex` (read-write lock).

There are only 4 operations which incur read lock:

- Read document ( __without__ JSON parsing)
- Scan collection ( __without__ JSON parsing)
- Scan hash table (all entries)
- Scan hash table (by hash key)

And only 5 operations incur write lock:

- Insert/update/delete document ( __without__ JSON parsing)
- Put/remove hash entry

Nothing else locks!

## HTTP endpoint synchronization

Most of HTTP endpoints never lock, however there is a small number of operations which must "stop the world" to ensure safe operation - these operations block __all__ other HTTP endpoints until completed:

- Create/rename/drop/scrub collection
- Create/remove index

The synchronization behaviour is controlled by a `RWMutex` - "stop the world" operations put write lock on it, all other operations put read lock on it.

## HTTP service

tiedot HTTP service is powered by HTTP server in standard Golang library `net/http`.

The HTTP service:

- Listens on all network interfaces
- Listens on the port specified by user via CLI parameter
- Unconditionally processes all incoming requests
- Scalability is affected by `GOMAXPROCS`

See "HTTP API Reference" for documentation HTTP service usage.

Once tiedot enters HTTP service mode, it keeps running in foreground until:

- `/shutdown` endpoint is called (gracefully shutdown)
- Process is stopped/interrupted/killed (not good!)