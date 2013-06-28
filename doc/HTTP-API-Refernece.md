#HTTP API Reference

## General info

tiedot server serves one database instance and listens on all network interfaces for incoming HTTP requests.

All server endpoints support GET/PUT/POST methods. Request parameters may also be passed via any of those methods.

There are two types of endpoints - synchronized (stop-the-world) and asynchronized. Certain endpoints have to be synchronized in order to ensure safe operation, such as renaming a collection or removing an index.

Server response always contains `Cache-Control: must-revalidate` header.

## Generic error response

Server responds HTTP status 400 in any of these situations:

- Required parameter is not given a value (for example: collection name required but not given)
- Parameter value is in wrong format (for example: number required but text given)
- A server condition considers request inappropriate/incorrect (for example: trying to drop an inexisting collection)

HTTP status 500 indicates a severe server error (usually comes with log messages).

To help with diagnostics, any HTTP 4xx/5xx response will include error message text (__not__ JSON formatted).

## Collection management endpoints

All the endpoints below are __synchronized__.

<table>
  <tr>
    <th>Function</th>
    <th>Endpoint</th>
    <th>Required parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Create collection</td>
    <td>/create</td>
    <td>col=(collection name)</td>
    <td>HTTP 201</td>
  </tr>
  <tr>
    <td>Get all collection names</td>
    <td>/all</td>
    <td>(nil)</td>
    <td>HTTP 200<br/>(JSON array of collection names)</td>
  </tr>
  <tr>
    <td>Rename collection</td>
    <td>/rename</td>
    <td>old=(existing collection name)<br/>new=(new collection name)</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Drop collection</td>
    <td>/drop</td>
    <td>col=(collection name)</td>
    <td>HTTP 200</td>
  </tr>
  <tr>
    <td>Scrub collection</td>
    <td>/drop</td>
    <td>col=(collection name)</td>
    <td>HTTP 200</td>
  </tr>
</table>

## Query endpoints

All the endpoints below are __Asynchronized__.

<table>
  <tr>
    <th>Function</th>
    <th>Endpoint</th>
    <th>Required parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Query document content</td>
    <td>/query</td>
    <td>col=(collection name)<br/>q=(query in JSON)</td>
    <td>HTTP 200<br/>(JSON formatted documents, one on each line)</td>
  </tr>
  <tr>
    <td>Query document IDs</td>
    <td>/query</td>
    <td>col=(collection name)<br/>q=(query in JSON)</td>
    <td>HTTP 200<br/>(document ID integers, one on each line)</td>
  </tr>
  <tr>
    <td>Count query result</td>
    <td>/query</td>
    <td>col=(collection name)<br/>q=(query in JSON)</td>
    <td>HTTP 200<br/>(one integer, number of documents in query result)</td>
  </tr>
</table>

## Document management endpoints

All the endpoints below are __Asynchronized__.

<table>
  <tr>
    <th>Function</th>
    <th>Endpoint</th>
    <th>Required parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Insert document</td>
    <td>/insert</td>
    <td>col=(collection name)<br/>doc=(JSON document)</td>
    <td>HTTP 201<br/>(New document ID)</td>
  </tr>
  <tr>
    <td>Get a document</td>
    <td>/get</td>
    <td>col=(collection name)<br/>id=(document ID)</td>
    <td>HTTP 200<br/>(JSON document)</td>
  </tr>
  <tr>
    <td>Update a document</td>
    <td>/update</td>
    <td>col=(collection name)<br/>id=(document ID)<br/>doc=(JSON document)</td>
    <td>HTTP 200<br/>(New document ID, could be the same as original ID)</td>
  </tr>
  <tr>
    <td>Delete a document</td>
    <td>/delete</td>
    <td>col=(collection name)<br/>id=(document ID)</td>
    <td>HTTP 200</td>
  </tr>
</table>

## Index management endpoints

All the endpoints below are __synchronized__.

<table>
  <tr>
    <th>Function</th>
    <th>Endpoint</th>
    <th>Required parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Create index</td>
    <td>/index</td>
    <td>col=(collection name)<br/>path=(path segments, joined by comma)</td>
    <td>HTTP 201</td>
  </tr>
  <tr>
    <td>List all index in a collection</td>
    <td>/indexes</td>
    <td>col=(collection name)</td>
    <td>HTTP 200<br/>(JSON array of indexed paths)</td>
  </tr>
  <tr>
    <td>Remove an index</td>
    <td>/unindex</td>
    <td>col=(collection name)<br/>path=(path segments, joined by comma)</td>
    <td>HTTP 200<br/></td>
  </tr>
</table>

## Miscelenous endpoints

All the endpoints below are __synchronized__.

<table>
  <tr>
    <th>Function</th>
    <th>Endpoint</th>
    <th>Required parameters</th>
    <th>Normal response</th>
  </tr>
  <tr>
    <td>Shutdown server</td>
    <td>/shutdown</td>
    <td>(nil)</td>
    <td>No response<br/>Connection closed</td>
  </tr>
</table>
