## tiedot built-in benchmark

tiedot has three built-in benchmark cases. To invoke benchmark, compile and run tiedot with CLI parameter:

    ./tiedot -mode=bench   # benchmark 1
    ./tiedot -mode=bench2  # benchmark 2

The default benchmark sample size is 400,000 for all three cases; it can be changed via CLI parameter `-benchsize=<new_size>`.

### Benchmark 1

Invoked by `tiedot -mode=bench`, the benchmark prepares a collection with two indexes, and prepares a large sample of documents (all deserialized, which uses a LOT of memory), then runs:

- Insert documents (effective on both indexes)
- Read document at random locations
- Query - lookup on both indexes
- Update document at random locations
- Delete document at random locations

It is designed to test performance of each individual document operation, to assist in finding performance regressions. The result should accurately reflect batch CRUD operation performance.

### Benchmark 2

Invoked by `tiedot -mode=bench2`, the benchmark first prepares a collection with two indexes and 1000 documents, then do *all* these operations at the same time:

- Insert/update/delete documents
- Read documents and do lookup queries

Unlike Benchmark 1, Benchmark 2 does not require large amount of free memory even if a very large `benchsize` is given.

This benchmark focuses on concurrency, to reflect performance under mixed workloads.

## Available memory VS performance

tiedot runs well with even less than 100 MB of available memory during normal operations. Similar to other NoSQL solutions, having larger amount of free memory improves performance when the database is also large.

### When data size < available memory

This is the preferred situation - there is plenty memory available for holding all data files. Operating system does a very good on managing mapped file buffers, swapping rarely happens and there is minimal to no IO on disk. In this situation, tiedot performs as an in-memory database. The benchmark results on the home page of this wiki was collected under this scenario.

### When data size > available memory

This is not ideal - there is not enough memory to hold all collection data, memory buffer becomes less efficient due to frequent page faults.

When approximately half of collection data resides in virtual memory, the performance of mixed workloads drops by approximately 400%; depending on your virtual memory media (flash/conventional HDD), amount of available main memory and usage, actual performance may vary.

### Performance comparison with other NoSQL solutions

Every NoSQL solution has its own advantages and disadvantages. By offering feature simplicity, tiedot performs as well as (and very likely, faster than) mainstream NoSQL solutions, but tiedot does not offer some advanced capabilities such as replication and map-reduce (yet), in which case other solutions may be more capable of handling.