### 3.1 (not released yet)

New stable features:

- A beautiful web control panel application runs along with ordinary HTTP APIs.
- Easily scan through large collections using new HTTP and embedded APIs.

Improvements:

- Significantly improved startup performance thanks to Smart File Scan.
- Other improvements in error handling, locking, logging, and embedded usage examples.

Bug fix:

- Fix a bug that may cause inaccurate lookup query result to be returned.

### 3.0 (2014-05-31)

Bug fix:

- Fix numerous bugs that may cause inconsistent index state in version 2.x

Improvements:

- Move document ID out of document data itself, improving performance.
- Tweaked hash table initial parameters.

Be aware that:

- Version 3.0 is not compatible with older databases, unfortunately a migration tool is not available at the moment.

### 2.1 (2014-05-31)

New experimental features:

- JSON parameterized queries
- A Python client for HTTP APIs

### 2.0 (2014-01-20)

tiedot 2.0 brings to you new file structures, APIs and a much cleaner codebase. The essential new feature in 2.0 is that all collections are partitioned, hoping to show better scalability; but the mechanism is still being tweaked.

Unfortunately, it is not compatible with older 1.x databases.

### 1.2 (2013-12-31)

Bug fix:

- Better embedded usage examples.
- Code comments rewritten to be more readable.

Improvements:

- Benchmark sample size is now configurable via CLI parameters.
- Structures inside array may now be indexed as well.
- New HTTP service endpoint to dump database while staying online.
- New HTTP service endpoint to flush all data buffers.
- Verbose log messages can be turned on/off.

Be aware that:

- Original V1 and V2 HTTP APIs have been removed, together with their documents.
- Old query processor (used by V1 and V2 APIs) has been removed.

### 1.1 (2013-11-07)

Bug fix:

- Panic due to out of memory on several 32-bit machines.
- Fix several incorrect HTTP API content type.
- Fix wrong new bucket position in opened hash tables.

Improvements:

- Remove per-collection padding buffer and replace it by a shared string buffer, to reduce memory consumption.
- Creation of data file no longer creates a giant empty buffer beforehand, therefore reducing memory consumption.
- Documents may now have optional persistent IDs (called UID) which will never change during its life time.
- API version 3 (New and backward compatible) supports document operations based on UIDs.
- Lock granularity is further tweaked.
- When a document cannot be indexed due to having incompatible data structure, a warning message is logged.
- Query now supports regex collection scan and reversed integer range lookup.

### 1.0 (2013-09-21)

Another maintenance release to address all outstanding issues, with feature improvements and new APIs.

Bug fix:

- Scalability problem on a model of laptop has been resolved.
- Collection update will no longer panic under a rare data corruption situation.

Improvements:

- Data file IO now uses more granular locks (RWlock-per-bucket and RWlock-per-document) instead of giant RW file lock.
- API version 2 (New and backward compatible)
- New HTTP endpoints to report server runtime performance.
- New query syntax - easier and more efficient, together with a new query processor.
- A specific type of range query (integer lookup in a range) is now supported.

### beta (2013-07-12)

A maintenance release to address outstanding issues discovered in alpha.

Bug fixes:

- Data durability is greatly enhanced by periodically (every minute) synchronizing file buffers with storage device.
- Support durable write operations which flush all buffers immediately after collection operation.
- Fix wrong content type returned by several HTTP API endpoints.

Improvements:

- tiedot can now run on Windows platform.
- tiedot now has a web control panel for managing collections/indexes/documents and run queries.

### alpha (2013-06-28)

Initial release with several known issues:

- Under a rare and specific data corruption situation, document update may panic.
- Several HTTP API endpoints return incorrect content type.
- File buffers in memory are not periodically synchronized with underlying storage device.