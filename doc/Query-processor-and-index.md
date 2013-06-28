# Query processor and index

## Query processing workflow

    QueryProcessor
      Input:
        Query (serialized from JSON)
        Collection
      
      Begin:
        If there are only bare number:
          Return the bare numbers
        
        Determine query operation
        
        If operation = 'exist' (value existence test):
          If path is indexed:
            Return all indexed entries
          Else:
            Test every document and return
            
        If operation = '=' (value lookup):
          If path is indexed:
            Return index scan result
          Else:
            Look for the value in all documents and return
            
        If operation = 'n' (Set intersection):
          Process each sub-query
          Calculate and return intersection of sub-query results
        
        If operation = 'c' (Set complement):
          Process each sub-query
          Calculate and return complement of sub-query results
        
        If operation = 'u' (Set union):
          Process each sub-query
          Calculate and return union of sub-query results
        
        If operation = 'all' (All documents):
          Do fast collection scan* and return all documents

\* Fast collection scan quickly finds all document IDs, without processing document content.

## Hash table index

tiedot supports hash table index to assist value lookup queries. The implemented hash table is typical static hash table.

Index works on a "path" - a series of keys locating the indexed value; for example, path `a,b,c` will locates value `1` in document `{"a": {"b": {"c": 1}}}`.

The value to be indexed is first converted to string by `fmt.Sprint()` and then hashed using [sdbm][] algorithm.

## Index optimized queries

For the paths which you frequently do lookup queries on, setting up index will greatly improve their performance. The query processor avoids collection scan and uses index scan whenever possible.

tiedot does not yet support range queries due to lack of range index support - this will be addressed in the near future.

[sdbm]: http://www.cse.yorku.ca/~oz/hash.html