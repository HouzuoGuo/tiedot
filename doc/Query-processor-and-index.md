### Index assisted lookup queries

tiedot supports hash table index to assist value lookup queries. The implemented hash table is typical static hash table.

Hash index works on a "path" - a series of keys locating the indexed value; for example, path `a,b,c` will locate value `1` in document `{"a": {"b": {"c": 1}}}`.

The value to be indexed is first converted to string by `fmt.Sprint()` and then hashed using [sdbm][] algorithm.

If your queries frequently do lookup, then setting up hash table index will greatly improve their performance.

### Index assisted range queries

tiedot supports a special case of range query - integer range lookup, which is essentially a batch of hash table lookups.

Better range query support will be introduced in later releases with help from another type of index.

[sdbm]: http://www.cse.yorku.ca/~oz/hash.html