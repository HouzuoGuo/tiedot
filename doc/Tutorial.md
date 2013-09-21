# Quick Start: tiedot in 10 minutes

You will need:

- git
- Go (>= 1.1)
- A regular web browser OR curl/wget

## Run tiedot

    mkdir tiedot && cd tiedot
    export GOPATH=`pwd`
    go get loveoneanother.at/tiedot

    ./bin/tiedot -mode=v2 -dir=/tmp/MyDatabase -port=8080

## Basics

tiedot server serves one database; database is made of collections; collection is made of documents and indexes.

API requests (along with parameter values) may be sent using of GET, POST or PUT methods.

## Manage collections

Create two collections:

    > curl "http://localhost:8080/create?col=Feeds"
    > curl "http://localhost:8080/create?col=Votes"

What collections do I have now?

    > curl "http://localhost:8080/all"
    ["Feeds","Votes"]

Rename collection "Votes" to "Points":

    > curl "http://localhost:8080/rename?old=Votes&new=Points"

Drop (delete) collection "Points":

    > curl "http://localhost:8080/drop?col=Points"

Scrub (repair and compact) "Votes":

    > curl "http://localhost:8080/scrub?col=Feeds"

## Manage documents

Insert document:

    > curl --data-ascii doc='{"a": 1, "b": 2}' "http://localhost:8080/insert?col=Feeds"
    0 # new document ID

Read document:

    > curl "http://localhost:8080/get?col=Feeds&id=0"
    {"a":1,"b":2}

Update document:

    > curl --data-ascii doc='{"a": 3, "b": 4}' "http://localhost:8080/update?col=Feeds&id=0"
    0 # updated document's new ID

Delete document:

    > curl "http://localhost:8080/delete?col=Feeds&id=0"

## Manage indexes

Index helps with query execution, but adds a small cost to insert/update/delete operations.

For example: Put an index on `a,b,c` will help queries finding documents such as `{"a": {"b": {"c": 1}}}`.

Create an index:

    > curl "http://localhost:8080/index?col=Feeds&path=a,b,c"

What indexes do I have now?

    > curl "http://localhost:8080/indexes?col=Feeds"
    ["a,b,c"]

Remove an index:

    > curl "http://localhost:8080/unindex?col=Feeds&path=a,b,c"

## Queries

Prepare some documents:

    > curl --data-ascii doc='{"Title": "New Go release", "Source": "golang.org", "Age": 3}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Kitkat is here", "Source": "android.com", "Age": 2}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Slackware Beta", "Source": "slackware.com", "Age": 1}' "http://localhost:8080/insert?col=Feeds"

Looking for the article "New Go Release":

    > curl --data-ascii q='{"eq": "New Go release", "in": ["Title"]}' "http://localhost:8080/query?col=Feeds"
    {"Age":3,"Source":"golang.org","Title":"New Go release"}

Looking for article "New Go release" and "android.com" feeds:

    > curl --data-ascii q='[{"eq": "New Go release", "in": ["Title"]}, {"eq": "android.com", "in": ["Source"]}]' "http://localhost:8080/query?col=Feeds"
    {"Age":3,"Source":"golang.org","Title":"New Go release"}
    {"Age":2,"Source":"android.com","Title":"Kitkat is here"}

Looking for all but not feeds from "golang.org":

    > curl --data-ascii q='{"c": [{"eq": "golang.org", "in": ["Source"]}, "all"]}' "http://localhost:8080/query?col=Feeds"
    {"Age":2,"Source":"android.com","Title":"Kitkat is here"}
    {"Age":1,"Source":"slackware.com","Title":"Slackware Beta"}

Note that: `"all"` means "all documents"; `{"c": [ .. ]}` means "complement". 

Looking for young feeds (age between 1 and 3) from "slackware.com":

    > curl --data-ascii q='{"n": [{"eq": "slackware.com", "in": ["Source"]}, {"int-from": 1, "int-to": 3, "in": ["Age"]}]}' "http://localhost:8080/query?col=Feeds"
    {"Age":1,"Source":"slackware.com","Title":"Slackware Beta"}

Note that: `{"n": [ .. ]}` means "intersect"; `{"int-from": x, "int-to": y, "in": [ .. ] }` is an integer range lookup.

There is also `count` API to return number of results, and `queryID` to return document ID instead of content. Their usage is identical to regular `query` API.

## Wrap up

Let's gracefully shutdown server:

    > curl "http://localhost:8080/shutdown"