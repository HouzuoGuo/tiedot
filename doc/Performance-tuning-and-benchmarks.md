## When data size < available memory

This is the preferred situation - there is plenty memory available for holding all data files. Operating system does a very good on managing mapped file buffers, swapping rarely happens and there is minimal to no IO on disk. In this situation, tiedot performs like an in-memory database.

Some benchmark results are shown on the front-page of this wiki.

## When data size > available memory

This is not ideal - there is not enough memory to hold all collection data; although operating system does its very best on keeping frequently access pages in memory, but swapping becomes an inevitable performance killer - just like what you experience on other NoSQL solutions.

I carried out this benchmark on my laptop: by increasing benchmark load, memory is filled up with serialized JSON documents to be loaded into benchmark collection, and leaving less than 500MB of available memory for more than 1GB of data files. tiedot benchmark accesses documents at randomly distributed locations, rendering memory buffer ineffecient - this is the worst scenario!

And here are the results collected from multiple benchmark runs:
(Operations per second)
<table>
<tr>
  <th>Processor</th>
  <th>Insert</th>
  <th>Read</th>
  <th>Query</th>
  <th>Update</th>
  <th>Delete</th>
  <th></th>
</tr>
<tr>
  <td>Mobile Intel Core i7 (2nd Gen)</td>
  <td>6-20k</td>
  <td>10-90k</td>
  <td>11-31k</td>
  <td>4-7k</td>
  <td>7-25k</td>
  <td>My 3 years old laptop</td>
</tr>
</table>

## Performance of "immediate durability" operations

Normally, tiedot synchronizes memory buffers with disk files every minute.

When you require immediately guaranteed data durability, tiedot supports `durableInsert/durableUpdate/durableDelete` (in `db/col.go`) which make syscall `msync` immediately following collection operation - they are 10000x more costly to use ompare to normal insert/update/delete operations, therefore you may not want to use them too often!

## Performance comparison with other NoSQL solutions

Every NoSQL solution has its own advantages and disadvantages; tiedot is unique in its own way:

- Its design scales very well on SMP (symmetric multi-processing) machines.
- General usage does not comply with ACID.
- Scalability is affected by Golang scheduler.

Depending on your usage scenarios: by offering feature simplicity, tiedot performs as well as (and very likely, faster than) mainstream NoSQL solutions, but tiedot does not offer some advanced capabilities such as replication and map-reduce (yet), in which case other solutions may be more capable of handling.