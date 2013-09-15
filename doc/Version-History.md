### master - current

Bug fixes:

- Collection update will no longer panic under a rare data corruption situation.

Improvements:

- New HTTP API version V2 introduces new server maintenance endpoints, new query syntax together with a new query processor with greatly improved performance.
- Hash index now supports a special type of range query - integer range lookup.

Known issues:

- Web control panel does not yet support the new HTTP API version.

### Beta

A maintenance release to address outstanding bugs in alpha, with some minor function improvements:

- Data durability is greatly enhanced by periodically (every minute) synchronizing file buffers with storage device.
- Support durable write operations which flush all buffers immediately after collection operation.
- Fix wrong content type returned by several HTTP API endpoints.
- tiedot can now run on Windows platform.
- tiedot now has a web control panel for managing collections/indexes/documents and run queries.

There is still an outstanding issue unresolved in this release: Under a rare and specific data corruption situation, document update may panic.

### Alpha

Initial release with several known issues:

- Under a rare and specific data corruption situation, document update may panic.
- Several HTTP API endpoints return incorrect content type.
- File buffers in memory are not periodically synchronized with underlying storage device.
