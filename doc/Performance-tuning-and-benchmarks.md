## Built-in benchmark

tiedot performance is usually always limited by CPU power rather than disk IO capability.

tiedot has two built-in benchmark scenarios. To invoke benchmark, compile and run tiedot with CLI parameter:

    ./tiedot -mode=bench   # benchmark 1
    ./tiedot -mode=bench2  # benchmark 2

The first scenario runs document insert, read, update, delete, and lookup queries one after the other. It is designed to demonstrate database engine throughput in each individual document operation, also for catching performance regressions.

The second scenario runs all the above operations at the same time, demonstrating engine throughput under very mixed load; it is also used for catching unwanted behaviors caused by concurrency.

In both scenarios, each benchmark sample document has 5 indexes, and each query looks for 5 keys across 3 indexes. The default benchmark size is 400,000 documents, the size may be changed via CLI parameter `-benchsize=<new_size>`.

On an Intel i7 2.9GHZ mobile CPU, the two benchmarks demonstrate that tiedot can consistently achieve:

- 120k inserts per second
- 420k single-document reads per second
- 80k lookup queries per second
- 60k updates per second
- 140k deletes per second
- 220k operations per second under mixed load (bench2)

The throughput numbers are more than doubled when number of indexes is reduced to one.

## Available memory VS performance

tiedot does not require much free memory to run! It still performs reasonably well even if the system has less than 100MB of available memory.

Similar to other NoSQL solutions, tiedot takes advantage of larger available memory to buffer data files and improve throughput.

### When data size < available memory

It is advantageous to have sufficient memory for all the data set. When there is plenty of memory available, the operating system does a very good job at managing mapped file buffers, swapping rarely occurs and there is minimal disk IO activity. In this case, tiedot performs just like an in-memory cache backed by disk files.

### When data size > available memory

This is not an ideal situation because swapping may occur. Depending on the actual access/usage pattern, the performance may suffer by up to 400% (when only 50% of data set resides in memory) or suffer no impact at all (when regularly accessed data resides in memory).

When available memory is not adequate to accommodate half of the data set, depending on usage pattern, there is a potential for tiedot to generate massive disk IO activities (due to swapping) and slow down the entire system - the same issue happens to other NoSQL databases that utilize memory mapped files.

### Performance comparison with other NoSQL solutions

Every NoSQL solution has its own advantages and disadvantages. By offering feature simplicity, tiedot performs even faster than many mainstream NoSQL solutions, but tiedot does not offer some advanced capabilities such as replication and map-reduce (yet), in which case other solutions may be more capable of handling.
