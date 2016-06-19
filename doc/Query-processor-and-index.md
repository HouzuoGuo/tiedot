### Query processor - supported operations

Query is a JSON structure (object or array) made of query operations, set operations and sub-queries.

Here is the comprehensive list of all supported query operations:

<table>
  <tr>
    <td>Document ID number as string</td>
    <td>No operation, the ID number goes to result</td>
  </tr>
  <tr>
    <td>"all"</td>
    <td>Return all document IDs (slow!)</td>
  </tr>
  <tr>
    <td>{"eq": #, "in": [#], "limit": #}</td>
    <td>Index value lookup</td>
  </tr>
  <tr>
    <td>{"int-from": #, "int-to": #, "in": [#], "limit": #}</td>
    <td>Hash lookup over a range of integers</td>
  </tr>
  <tr>
    <td>{"has": [#], "limit": #}</td>
    <td>Return all documents that has the attribute set (not null)</td>
  </tr>
  <tr>
    <td>[sub-query1, sub-query2..]</td>
    <td>Evaluate union of sub-query results.</td>
  </tr>
  <tr>
    <td>{"n": [sub-query1, sub-query2..]}</td>
    <td>Evaluate intersection of sub-query results.</td>
  </tr>
  <tr>
    <td>{"c": [sub-query1, sub-query2..]}</td>
    <td>Evaluate complement of sub-query results.</td>
  </tr>
</table>

`limit` is optional. Sub-query may have arbitrary complexity.

### Query example

The following example demonstrates how to query on the basis of a native array and a JSON-string:

```
// Instantiate database
DB := db.OpenDB("/path/to/your/db")
DB.Create("Users")
users := DB.Use("Users")

// Add index
err = users.Index([]string{"username"})
if nil != err {
    panic(err)
}


// Native Array
query = map[string]interface{}{
   "eq":    "JohnAppleseed",
   "in":    []interface{}{"username"},
   "limit": 1,
}
// OR JSON
var query interface{}
ffjson.Unmarshal([]byte(`[{"eq": "JohnAppleseed", "in": ["username"], "limit": 1}]`), &query)


// Evaluate the query
queryResult := make(map[int]struct{})
if err := db.EvalQuery(query, users, &queryResult); nil != err {
    panic(err)
}

// Fetch the results
for id := range queryResult {
    readBack, err := users.Read(id)
    if nil != err {
        panic(err)
    }
    fmt.Printf("Query returned document %v\n", readBack)
}
```

### Lookup queries

Indexes works on a "path" - a series of attribute names locating the indexed value, for example, path `a,b,c` will locate value `1` in document `{"a": {"b": {"c": 1}}}`.

If the index path visits or ultimately leads to an array of values, every value element will be indexed and a lookup query will match any value in the array. For example, an index on "Name,Pen Name" will index all of "John", "David", "Joshua" in the following document: 

    { "Name: [
        {"Pen Name": [ "John", "David" ]},
        {"Pen Name": "Joshua"}
    ] }

Index must be available before carrying out lookup queries.

### Index assisted range queries

tiedot supports a special case of range query - integer range lookup, which is essentially a batch of hash table lookups.

Better range query support will be introduced in later releases with help from another type of index.