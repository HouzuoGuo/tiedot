## Query processor workflow (processor version 2)

    QueryProcessor
      Input:
        Query (serialized from JSON)
        A Collection
      
      Begin:
        If there are bare numbers:
          Return the bare numbers in result

        If this is a list of sub-queries:
          Evaluate each sub-query and return their results combined.
        
        This is a value existence test?
          If path is indexed:
            Return all indexed entries
          Else:
            Test every document and return
            
        This is a value lookup?
          If path is indexed:
            Return index scan result
          Else:
            Look for the value in all documents and return
            
        This is an intersection?
          Process each sub-query
          Calculate and return intersection of sub-query results
        
        This is a complement?
          Process each sub-query
          Calculate and return complement of sub-query results

        This is a range query - integer lookup?
          For each integer in the range, do a value lookup and return results combined.
        
        Need all documents?
          Do fast collection scan* and return all documents as result

\* Fast collection scan quickly finds all document IDs, without processing document content.

Version 1 query processor (for HTTP API v1) works slightly differently. For more details on query syntax, please refer to HTTP API reference.

## Hash table index

tiedot supports hash table index to assist value lookup queries. The implemented hash table is typical static hash table.

Index works on a "path" - a series of keys locating the indexed value; for example, path `a,b,c` will locate value `1` in document `{"a": {"b": {"c": 1}}}`.

The value to be indexed is first converted to string by `fmt.Sprint()` and then hashed using [sdbm][] algorithm.

## Index optimized queries

For the paths which you frequently do lookup queries on, setting up index will greatly improve their performance. The query processor avoids collection scan and uses index scan whenever possible.

tiedot supports a special case of range query - integer range lookup, which is essentially a batch of hash table lookups.

Better range query support will be introduced in later releases with help from another type of index.

[sdbm]: http://www.cse.yorku.ca/~oz/hash.html