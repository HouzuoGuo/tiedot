# Quick Start: tiedot in 10 minutes

You will need:

- git
- Go (>= 1.1)
- HTTP client (web browser, curl, etc)

## Get tiedot running

This tutorial uses HTTP API version 1:

    mkdir tiedot && cd tiedot
    export GOPATH=`pwd`
    go get loveoneanother.at/tiedot

    # run server
    ./bin/tiedot -mode=v1 -dir=/tmp/MyDatabase -port=8080

    # tiedot is now running in Foreground, listening on port 8080

## Basics

tiedot server connects to a database; database is made of collections; collection is made of documents and indexes.

You may send HTTP requests (along with parameter values) using any of GET, POST or PUT methods. For an example:

    curl "http://localhost:8080/create?col=A"

## Manage collections

<table style="font-family: monospace;">
  <tr>
    <th></th>
    <th>Parameters</th>
    <th>Endpoint</th>
    <th>Response</th>
  </tr>
  <tr>
    <td>Create a collection</td>
    <td>col=Feeds</td>
    <td>/create</td>
    <td>(nil)</td>
  </tr>
  <tr>
    <td>Create another collection</td>
    <td>col=Votes</td>
    <td>/create</td>
    <td>(nil)</td>
  </tr>
  <tr>
    <td>Which collections do I have?</td>
    <td>(nil)</td>
    <td>/all</td>
    <td>["Feeds", "Votes"]</td>
  </tr>
  <tr>
    <td>Rename collection "Votes" to "Points"</td>
    <td>old=Votes<br/>new=Points</td>
    <td>/rename</td>
    <td>(nil)</td>
  </tr>
  <tr>
    <td>Drop collection "Points"</td>
    <td>col=Points<br/></td>
    <td>/drop</td>
    <td>(nil)</td>
  </tr>
  <tr>
    <td>Scrub(*) collection "Feeds"</td>
    <td>col=Feeds<br/></td>
    <td>/scrub</td>
    <td>(nil)</td>
  </tr>
</table>

\* Scrub is a maintenance command, it automatically gets rid of corrupted/deleted documents.

## Manage documents

<table style="font-family: monospace;">
  <tr>
    <th></th>
    <th>Parameters (BEFORE encoding)</th>
    <th>Endpoint</th>
    <th>Response</th>
  </tr>
  <tr>
    <td>Insert a document</td>
    <td>col=Feeds<br />doc={"a": 1, "b": 2}</td>
    <td>/insert</td>
    <td>0 (new document ID)</td>
  </tr>
  <tr>
    <td>Read a document</td>
    <td>col=Feeds<br />id=0</td>
    <td>/get</td>
    <td>{"a": 1, "b": 2}</td>
  </tr>
  <tr>
    <td>Update a document</td>
    <td>col=Feeds<br />id=0<br />doc={"a": 2, "b": 2}</td>
    <td>/update</td>
    <td>0 (updated document ID)</td>
  </tr>
  <tr>
    <td>Delete a document</td>
    <td>col=Feeds<br />id=0</td>
    <td>/delete</td>
    <td>(nil)</td>
  </tr>
</table>

## Manage indexes

Index helps speeding up lookup queries, but adds a small cost to insert/update/delete operations.

For example, if you index path "a,b,c", it will help queries finding documents like `{"a": {"b": {"c": 1}}}`.

<table style="font-family: monospace;">
  <tr>
    <th></th>
    <th>Parameters</th>
    <th>Endpoint</th>
    <th>Response</th>
  </tr>
  <tr>
    <td>Create an index</td>
    <td>col=Feeds<br />path=a,b,c</td>
    <td>/index</td>
    <td>(nil)</td>
  </tr>
  <tr>
    <td>What indexes do I have?</td>
    <td>col=Feeds</td>
    <td>/indexes</td>
    <td>["a,b,c"]</td>
  </tr>
  <tr>
    <td>Remove an index</td>
    <td>col=Feeds<br />path=a,b,c</td>
    <td>/unindex</td>
    <td>(nil)</td>
  </tr>
</table>

## Queries

Query is a JSON structure of __nested__ array and objects. tiedot supports some very basic query operations, however powerful queries can be built by combining different operations:

<table style="font-family: monospace;">
  <tr>
    <th>Operation</th>
    <th>Usage &amp; Example</th>
  </tr>
  <tr>
    <td>Value lookup</td>
    <td>["=", {"eq": VALUE, "limit": N, "in": ["path1", "path2"...]}]<br/><br/><i>["=", {"eq": "A", "limit": 1, "in": ["exam", "result", "CS"]}]</i></td>
  </tr>
  <tr>
    <td>Get all documents</td>
    <td>["all"]<br/><br/><i>["all"]</i></td>
  </tr>
  <tr>
    <td>Union</td>
    <td>["u", result1, result2...]<br/><br/><i>["u", ["=", {"eq": "David", "in": ["name"]}], ["=", {"eq": "Joe", "in": ["name"]}]]</i></td>
  </tr>
  <tr>
    <td>Intersect</td>
    <td>["n", result1, result2...]<br/><br/><i>["n", ["=", {"eq": "A", "in": ["math"]}], ["=", {"eq": "A", "in": ["piano"]}]]</i></td>
  </tr>
  <tr>
    <td>Complement</td>
    <td>["c", result1, result2...]<br/><br/><i>["c", ["all"], ["=", {"eq": "F", "in": ["math"]}]]</i></td>
  </tr>
</table>

Those set operations (union, intersect and complement) operate on a number of sub-query results, they are very helpful in building complicated queries.

The following endpoints support query operations:

<table style="font-family: monospace;">
  <tr>
    <th></th>
    <th>Parameters</th>
    <th>Endpoint</th>
    <th>Response</th>
  </tr>
  <tr>
    <td>Return document contents</td>
    <td>col=Feeds<br />q=YOUR_QUERY</td>
    <td>/query</td>
    <td>(documents, one on each line)</td>
  </tr>
  <tr>
    <td>Return document IDs</td>
    <td>col=Feeds<br />q=YOUR_QUERY</td>
    <td>/queryID</td>
    <td>(document IDs, one on each line)</td>
  </tr>
  <tr>
    <td>Return number of results</td>
    <td>col=Feeds<br />q=YOUR_QUERY</td>
    <td>/count</td>
    <td>(a positive integer)</td>
  </tr>
</table>

## Other stuff

Although tiedot data structures are very robust, but please gracefully shutdown server by making an HTTP request to endpoint `/shutdown`.

You can run a benchmark to see how well tiedot performs on your machine:

    ./tiedot -mode=bench

The benchmark suit is large and may take a minute or two to complete.
