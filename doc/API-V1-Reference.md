## General info

API requests may use any of GET/PUT/POST methods.

Server response always contains `Cache-Control: must-revalidate` header. Most responses use `applicaton/json` content type, but there are exceptions.

The V1 API is less efficient and query syntax is more complicated, therefore please use [V2 API] wherever possible.

To start this API server, please pass CLI parameter `-mode=v1`.

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
    <td>Get a document</td>
    <td>/get</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200 and a JSON document</td>
  </tr>
  <tr>
    <td>Update a document</td>
    <td>/update</td>
    <td>Collection name `col`, document ID `id` and new JSON document `doc`</td>
    <td>HTTP 200 and updated document ID (text/plain)</td>
  </tr>
  <tr>
    <td>Delete a document</td>
    <td>/delete</td>
    <td>Collection name `col` and document ID `id`</td>
    <td>HTTP 200</td>
  </tr>
</table>

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
    <td>Version number</td>
    <td>/version</td>
    <td>(nil)</td>
    <td>HTTP 200 and "1" (text/plain)</td>
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

[V2 API]: https://github.com/HouzuoGuo/tiedot/wiki/API-V2-Reference