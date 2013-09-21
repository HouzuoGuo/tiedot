### 1.0 (2013-09-21)

Another maintenance release to address all outstanding issues, with feature improvements and new APIs.

Bug fix:

- Collection update will no longer panic under a rare data corruption situation.

Improvements - API version 2 (New!):

- New HTTP endpoints to report server runtime performance.
- New query syntax - easier and more efficient, together with a new query processor.
- A specific type of range query (integer lookup in a range) is now supported.

Known issues: None

### beta (2013-07-12)

A maintenance release to address outstanding issues discovered in alpha.

Bug fixes:

- Data durability is greatly enhanced by periodically (every minute) synchronising file buffers with storage device.
- Support durable write operations which flush all buffers immediately after collection operation.
- Fix wrong content type returned by several HTTP API endpoints.

Improvements:

- tiedot can now run on Windows platform.
- tiedot now has a web control panel for managing collections/indexes/documents and run queries.

Known issue:

- Under a rare and specific data corruption situation, document update may panic.

### alpha (2013-06-28)

Initial release.

Known issues:

- Under a rare and specific data corruption situation, document update may panic.
- Several HTTP API endpoints return incorrect content type.
- File buffers in memory are not periodically synchronised with underlying storage device, thus reducing data durability.