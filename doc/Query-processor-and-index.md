### Query processor - supported operations

Query is a JSON structure (object or array) made of operations, including sub-queries.

Here is the complete list of all supported operations:

<table>
  <tr>
    <td>ID number as a string</td>
    <td>No operation, the ID number goes to result</td>
  </tr>
  <tr>
    <td>"all"</td>
    <td>Return all document IDs</td>
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
    <td>Evaluate and union sub-query results.</td>
  </tr>
  <tr>
    <td>{"n": [sub-query1, sub-query2..]}</td>
    <td>Evaluate and intersect sub-query results.</td>
  </tr>
  <tr>
    <td>{"c": [sub-query1, sub-query2..]}</td>
    <td>Evaluate and complement sub-query results.</td>
  </tr>
  <tr>
    <td>{"re": #, "limit": #}</td>
    <td>Return all documents that match the regex.</td>
  </tr>
</table>

Limits are optional.

### Lookup queries

Document unique IDs are indexed on primary key. Secondary indexes works on a different "path" - a series of attribute names locating the indexed value, for example, path `a,b,c` will locate value `1` in document `{"a": {"b": {"c": 1}}}`.

Secondary index should be available before carrying out lookup queries.

### Index assisted range queries

tiedot supports a special case of range query - integer range lookup, which is essentially a batch of hash table lookups.

Better range query support will be introduced in later releases with help from another type of index.