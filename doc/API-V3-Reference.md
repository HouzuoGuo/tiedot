## General info

API requests may use any of GET/PUT/POST methods.

Server response always contains `Cache-Control: must-revalidate` header. Most responses use `applicaton/json` content type, but there are exceptions.

To start this API server, please pass CLI parameter `-mode=v3`.

## Generic error response

Server returns HTTP status 400 when:

- Required parameter does not have a value (e.g. ID is required but not given).
- Parameter does not contain correct value data type (e.g. ID should be a number, but letter S is given).
- It is not possible to full-fill the request (e.g. inserting a new document that is not JSON).

When internal error occurs, server will log a message and return HTTP status 500.

HTTP 4xx/5xx response will always include a text message (not JSON) to help with diagnostics.

## Collection management

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Create a collection</td>
    <td>/create</td>
    <td>Collection name `col`</td>
    <td>HTTP 201</td>
  </tr>
  <tr>
    <td>Get all collection names</td>
    <td>/all</td>
    <td></td>
    <td>HTTP 200 and a JSON array of all collection names</td>
  </tr>
  <tr>
    <td>Rename a collection</td>
    <td>/rename</td>
    <td>Original name `old` and new name `new`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Drop a collection</td>
    <td>/drop</td>
    <td>Collection name `col`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Scrub (repair and compact) collection</td>
    <td>/scrub</td>
    <td>Collection name `col`</td>
    <td>HTTP 200</td>
  </tr>
</table>

## Document management

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Insert a document</td>
    <td>/insert</td>
    <td>Collection name `col` and JSON document string `doc`</td>
    <td>HTTP 201 and new document ID (text/plain)</td>
  </tr>
  <tr>
    <td>Insert a document<br/>(with an auto UID)</td>
    <td>/insertWithUID</td>
    <td>Collection name `col` and JSON document string `doc`</td>
    <td>HTTP 201 and JSON object of new document's ID and UID</td>
  </tr>
  <tr>
    <td>Get a document</td>
    <td>/get</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200 and a JSON document</td>
  </tr>
  <tr>
    <td>Get a document by UID</td>
    <td>/getByUID</td>
    <td>Collection name `col` and document UID `uid`</td>
    <td>HTTP 200 and a JSON object of document content `doc` and ID</td>
  </tr>
  <tr>
    <td>Update a document</td>
    <td>/update</td>
    <td>Collection name `col`, document ID `id` and new JSON document `doc`</td>
    <td>HTTP 200 and updated document ID (text/plain)</td>
  </tr>
  <tr>
    <td>Update a document by UID</td>
    <td>/updateByUID</td>
    <td>Collection name `col`, document UID `uid` and new JSON document `doc`</td>
    <td>HTTP 200 and updated document ID (text/plain)</td>
  </tr>
  <tr>
    <td>(Re)assign document UID</td>
    <td>/reassignUID</td>
    <td>Collection name `col`, document ID `id`</td>
    <td>HTTP 200 and JSON object of new document ID and UID</td>
  </tr>
  <tr>
    <td>Delete a document</td>
    <td>/delete</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Delete a document by UID</td>
    <td>/deleteByUID</td>
    <td>Collection name `col` and document UID `uid`</td>
    <td>HTTP 200</td>
  </tr>
</table>

Document ID is the physical location of document, it uniquely identifies document but may change during its life time. UID is another unique identifier, it is persistent and does not change over time.

## Index management

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Create index</td>
    <td>/index</td>
    <td>Collection name `col` and index path (comma separated string) `path`</td>
    <td>HTTP 201</td>
  </tr>
  <tr>
    <td>Get details of all indexes in a collection</td>
    <td>/indexes</td>
    <td>Collection name `col`</td>
    <td>HTTP 200 and a JSON array of all index information</td>
  </tr>
  <tr>
    <td>Remove an index</td>
    <td>/unindex</td>
    <td>Collection name `col` and index path to be removed (comma separated string) `path`</td>
    <td>HTTP 200<br/></td>
  </tr>
</table>

## Server management

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Shutdown server</td>
    <td>/shutdown</td>
    <td>(nil)</td>
    <td>No response and connection is closed</td>
  </tr>
  <tr>
    <td>Server runtime memory statistics</td>
    <td>/memstats</td>
    <td>(nil)</td>
    <td>HTTP 200 and `runtime.MemStats` serialised into JSON</td>
  </tr>
  <tr>
    <td>Version number</td>
    <td>/version</td>
    <td>(nil)</td>
    <td>HTTP 200 and "3" (text/plain)</td>
  </tr>
</table>


## Query endpoints

All query responses return content type `text/plain` instead of `application/json`.

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Get document content</td>
    <td>/query</td>
    <td>Collection `col` and query string `q`</td>
    <td>HTTP 200 and result documents (one per line)</td>
  </tr>
  <tr>
    <td>Get document IDs</td>
    <td>/queryID</td>
    <td>Collection `col` and query string `q`</td>
    <td>HTTP 200 and result docment IDs (one per line)</td>
  </tr>
  <tr>
    <td>Count results</td>
    <td>/count</td>
    <td>Collection `col` and query string `q`</td>
    <td>HTTP 200 and an integer number</td>
  </tr>
</table>

### Query syntax

Query is a JSON structure, made of query operation, query parameters, sub-queries and bare numbers. Query processor takes a collection and query as input, and produces a set of result document IDs as output.

#### Bare numbers

Bare numbers go directly into query result, this may be useful for manually injecting document IDs into a query. For example: `1, 2, 3, 4`.

#### Basic operations

Lookup finds documents with specific value in a path: `{"in": [ path ... ], "eq": loookup_value}`.

For example: `{"in": ["Author", "Name", "First Name"], "eq": "John"}`.

You can also find documents by using regex: `{"in": [ path ... ], "re": "Go regular expression"}`.

For example: `{"in": ["Author, "Name", "First Name"], "re": "^John.*ed$"}`

Another operation, "has", finds any document with any value in the path: `{"has": [ path ...] }`.

For example: `{"has": ["Author", "Name", "Pen Name"]}`.

Integer range query is also supported: `{"in": [ path ... ], "int-from": xx, "int-to": yy}`

For example: `{"in": ["Publish", "Year"], "int-from": 1993, "int-to": 2013}`

`"all"` returns all document IDs, may be useful for set operation (especially, complement).

#### Set operations

Set operations take a list of queries (sub-queries) as parameter, the sub-queries may have any arbitrary complexity. 

- Intersection: `{"n": [ sub-queries ... ]}`
- Complement: `{"c": [ sub-queries ... ]}`
- Union: `[ sub-queries ...]`

Here is a complicated example: Find all books which were not written by John and published between 1993 and 2013, but include those written by John in 2000.

	[
		{
			"n": [
				{ "in": [ "Author", "Name" ], eq": "John" },
				{ "in": [ "Publish", "Year" ], "eq": 2000 }
			]
		},
		{
			"c": [
				"all",
				{ "n": [
						{ "in": [ "Author", "Name" ], "eq": "John" },
						{ "in": [ "Publish", "Year" ], "int-from": 1993, "int-to": 2013 }
					]
				}
			]
		}
	]