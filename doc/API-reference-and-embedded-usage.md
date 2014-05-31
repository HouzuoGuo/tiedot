## General info

API requests may use any of GET/PUT/POST methods.

Server response always contains `Cache-Control: must-revalidate` header. Most responses use `applicaton/json` content type, but there are exceptions.

To start HTTP service, please run tiedot with CLI parameter `-mode=httpd`.

## Generic error response

Server returns HTTP status 400 when:

- Required parameter does not have a value (e.g. ID is required but not given).
- Parameter does not contain correct value data type (e.g. ID should be a number, but letter S is given).

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
    <td>Collection name `col` and number of partitions `numparts`</td>
    <td>HTTP 201</td>
  </tr>
  <tr>
    <td>Get all collection names</td>
    <td>/all</td>
    <td></td>
    <td>HTTP 200 and a JSON object of all collection names and configuration</td>
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
  <tr>
    <td>Flush all database file</td>
    <td>/sync</td>
    <td>(nil)</td>
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
    <td>HTTP 201 and new document ID</td>
  </tr>
  <tr>
    <td>Get a document</td>
    <td>/get</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200 and a JSON document</td>
  </tr>
  <tr>
    <td>Update a document</td>
    <td>/update</td>
    <td>Collection name `col`, document ID `id` and new JSON document `doc`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Delete a document</td>
    <td>/delete</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200</td>
  </tr>
</table>

Document ID is unique and never changes til the document vanishes.

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
    <td>Get list of all indexes in a collection</td>
        <td>/indexes</td>
    <td>Collection name `col`</td>
    <td>HTTP 200 and a JSON array of all indexed paths</td>
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
    <td>Dump (backup) database*</td>
    <td>/dump</td>
    <td>Destination directory `dest`</td>
    <td>HTTP 200</td>
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
    <td>HTTP 200 and "4"</td>
  </tr>
</table>

\* Further requests will not be processed until dumping is completed.

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
    <td>Execute query and return documents</td>
    <td>/query</td>
    <td>Collection `col` and query string `q`</td>
    <td>HTTP 200 and result document IDs and content</td>
  </tr>
  <tr>
    <td>Execute query and count results</td>
    <td>/count</td>
    <td>Collection `col` and query string `q`</td>
    <td>HTTP 200 and an integer number</td>
  </tr>
</table>

### Query syntax

Query is a JSON structure, made of query operation, query parameters, sub-queries and bare strings. Query processor takes a collection and query as input, and produces document contents or count of results, depending on HTTP endpoint.

#### Bare strings (with number content)

Bare strings are Document IDs that go directly into query result, this may be useful for manually injecting document IDs into a query. For example: `["23101561275236320", "2461300515680780859"]`.

#### Basic operations

Lookup finds documents with specific value in a path: `{"in": [ path ... ], "eq": loookup_value}`.

For example: `{"in": ["Author", "Name", "First Name"], "eq": "John"}`.

Another operation, "has", finds any document with any not-null value in the path: `{"has": [ path ...] }`.

For example: `{"has": ["Author", "Name", "Pen Name"]}`.

Integer range query is also supported (here you may also use `limit`): `{"in": [ path ... ], "int-from": xx, "int-to": yy}`

For example: `{"in": ["Publish", "Year"], "int-from": 1993, "int-to": 2013, "limit": 10}`

`"all"` returns all document IDs, may be useful for set operation (especially, complement).

_Lookup paths must be indexed._

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

## Embedded usage
APIs for embedded usage are demonstrated in `example.go`, you may run the example by building tiedot and run:

    ./tiedot -mode=example