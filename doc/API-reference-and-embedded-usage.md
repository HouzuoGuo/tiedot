## General info

You may use any HTTP methods (supported by Go HTTP server) to make API requests.

Server response will always have `Cache-Control: must-revalidate` header. Most responses return `applicaton/json` content type, but there are exceptions. All API endpoints are safe for concurrent usage.

To start HTTP server, run tiedot with CLI parameters: `-mode=httpd -dir=path_to_db_directory -port=port_number`

To enable HTTPS and disable HTTP, add additional parameters: `-tlskey=keyfile -tlscrt=crtfile`.

To enable mandatory JWT (Javascript Web Token) authorization on all API calls, add additional parameters: `-jwtprivatekey=keyfile2 -jwtpubkey=pubkeyfile`.

The "rsa-test" key-pair in tiedot source code is for testing purpose only, please refrain from using it to start HTTPS server or to enable JWT.

## General error response

Server may respond with HTTP status 400 when:

- A required parameter does not have a value (e.g. ID is required but not given).
- A parameter does not contain correct value data type (e.g. ID should be a number, but letter S is given).

When internal error occurs, server will respond with an error message (plain text) and HTTP status 500; it may also log more details in standard output and/or standard error.

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
    <td>HTTP 200 and a JSON array of collection names</td>
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
    <td>Scrub (compact and repair) collection</td>
    <td>/scrub</td>
    <td>Collection name `col`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Immediately synchronize all data files*</td>
    <td>/sync</td>
    <td>(nil)</td>
    <td>HTTP 200</td>
  </tr>
</table>

\* All data files are automatically synchronized every 2 seconds.

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
    <td>HTTP 201 and new document ID*</td>
  </tr>
  <tr>
    <td>Get a document</td>
    <td>/get</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200 and a JSON object (the document)</td>
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
  <tr>
    <td>Get approx. count of documents</td>
    <td>/approxdoccount</td>
    <td>Collection name `col</td>
    <td>HTTP 200 and an integer number</td>
  </tr>
  <tr>
    <td>Get a page of documents**</td>
    <td>/getpage</td>
    <td>Collection name `col`, page number `page` and total number of pages `total`</td>
    <td>HTTP 200 and JSON objects (the documents)</td>
  </tr>
</table>

\* Document ID is an automatically generated unique ID. It remains unchanged for the document until the document is deleted.

\** "getpage" divides all documents roughly equally large "pages". It is useful for doing collection scan. To calculate total number of pages, first decide how many documents you would like to see in a page, then calculate `"approxdoccount" / DOCS_PER_PAGE`. The documents in HTTP response reflect storage layout and are not ordered.

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
    <td>Dump (backup) database</td>
    <td>/dump</td>
    <td>Destination directory `dest`</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Shutdown server</td>
    <td>/shutdown</td>
    <td>(nil)</td>
    <td>Connection is closed, no response</td>
  </tr>
  <tr>
    <td>Get Go memory allocator statistics</td>
    <td>/memstats</td>
    <td>(nil)</td>
    <td>HTTP 200 and `runtime.MemStats` serialized into JSON</td>
  </tr>
  <tr>
    <td>Version number</td>
    <td>/version</td>
    <td>(nil)</td>
    <td>HTTP 200 and "6"</td>
  </tr>
</table>

## JWT - Javascript Web Token

Launch tiedot HTTP server with JWT will enable mandatory JWT authorization on all API endpoints. The general operation flow is following:

- tiedot HTTP server starts up with JWT enabled.
- Client calls `/getjwt?user=user_name&pass=password` and then memorize the "Authorization" header from response. This is your JW token containing your access rights. The token is encrypted by tiedot server and can only be decrypted by tiedot server. The token is valid for 72 hours.
- Client makes subsequent API calls to JWT-enabled HTTP endpoints, with an additional Authorization header (the return value of `getjwt`) in each request.

<table>
  <tr>
    <th>Function</th>
    <th>URL</th>
    <th>Parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Acquire a new token</td>
    <td>/getjwt</td>
    <td>User name `user` and password `pass</td>
    <td>HTTP 200 and the new token is in Authorization header</td>
  </tr>
  <tr>
    <td>Check a token's validity</td>
    <td>/checkjwt</td>
    <td>Token in request Authorization header</td>
    <td>HTTP 200</td>
  </tr>
</table>

### User and access rights management in JWT

An HTTP client acquires JW token by calling `/getjwt` with parameter `user` and `pass`. Upon enabling JWT for the first time on a database, the default user "admin" with empty password is created, and the following message will be shown during tiedot startup:

    JWT: successfully initialized DB for JWT features. The default user 'admin' has been created.

The database collection `jwt` stores user, password, and access rights information for all users, including "admin". "admin" is a special user not restrained by access rights assignment.

You are free to manipulate collection `jwt` to manage users, passwords, and access rights. Each document should look like:

    {
        "user": "user_name",
        "pass": "password_plain_text",
        "endpoints": [
            "create",
            "drop",
            "insert",
            "query",
            "update",
            "other_api_endpoint_names..."
        ],
        "collections: [
            "collection_name_A",
            "collection_name_B",
            "other_collection_names..."
        ]
    }

Password is in plain-text, you are free to use a randomly generated password, or a hashed password in an algorithm of your choice.

## Query

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

Query string is in JSON; it may consist of operators, query parameters, sub-queries and bare-strings. These are the supported query operations (from fastest to slowest):

- Direct document ID (no processing involved)
- Value lookup (field=value)
- Value lookup over integer range (field=1,2,3,4)
- Path existence test (field has value)
- Get all document IDs

There are also set operations - intersect, union, difference, complement; the set operations are very fast.

#### Bare strings (document IDs)

Bare strings are Document IDs that go directly into query result. For example: `["23101561275236320", "2461300515680780859"]`.

#### Basic operations

Lookup finds documents with a specific value in a path: `{"in": [ path ... ], "eq": loookup_value}`.

For example: `{"in": ["Author", "Name", "First Name"], "eq": "John"}`.

Another operation, "has", finds any document with not-null value in the path: `{"has": [ path ...] }`.

For example: `{"has": ["Author", "Name", "Pen Name"]}`.

Integer range query is also supported: `{"in": [ path ... ], "int-from": xx, "int-to": yy}`

For example: `{"in": ["Publish", "Year"], "int-from": 1993, "int-to": 2013, "limit": 10}`

All of the above queries may use an optional "limit" key (for example "limit": 10) to limit number of returned result.

Note that:

- Use "limit": 1 if you intend to get only one result document, this will significantly improve performance.
- Query paths involved in lookup and "has" queries must be indexed beforehand.
- A special operation "all" (bare-string) will return all document IDs; it is the slowest operation of all, but may prove useful in certain set operations such as complement of sets.

#### Set operations

Set operations take a list of sub-queries as parameter, the sub-queries may be arbitrarily complex.

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

tiedot is designed for ease-of-use in both HTTP API and embedded usage. Embedded usage is demonstrated in `example.go`, see the source code comments for details.