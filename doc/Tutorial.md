# Tutorial: tiedot in 10 minutes

You will need:

- git
- Go (>= 1.1)
- A regular web browser OR curl/wget

## Run tiedot

    mkdir tiedot && cd tiedot
    export GOPATH=`pwd`  # backticks surround pwd
    go get github.com/HouzuoGuo/tiedot

    ./bin/tiedot -mode=httpd -dir=/tmp/MyDatabase -port=8080

## Basics

tiedot server serves one database; database is made of collections; collection is partitioned (to improve performance) and has indexes to assist queries. Each document has a unique ID number called "@id" that never change.

API requests (along with parameter values) may be sent using of GET, POST or PUT methods.

## Manage collections

Create two collections:

    > curl "http://localhost:8080/create?col=Feeds&numparts=2"
    > curl "http://localhost:8080/create?col=Votes&numparts=2"

What collections do I have now?

    > curl "http://localhost:8080/all"
    {"Feeds":{"partitions":2},"Votes":{"partitions":2}}

Rename collection "Votes" to "Points":

    > curl "http://localhost:8080/rename?old=Votes&new=Points"

Drop (delete) collection "Points":

    > curl "http://localhost:8080/drop?col=Points"

## Manage documents

Insert document:

    > curl --data-ascii doc='{"a": 1, "b": 2}' "http://localhost:8080/insert?col=Feeds"
    11355681827558540738 # a random number - the new document's unique ID

Read document:

    > curl "http://localhost:8080/get?col=Feeds&id=11355681827558540738"
    {"@id":"11355681827558540738","a":1,"b":2}

Update document:

    > curl --data-ascii doc='{"a": 3, "b": 4}' "http://localhost:8080/update?col=Feeds&id=11355681827558540738"

Delete document:

    > curl "http://localhost:8080/delete?col=Feeds&id=11355681827558540738"

## Manage indexes

Index is required by all lookup queries. 

For an example: Put an index on `a,b,c` will help queries finding documents such as `{"a": {"b": {"c": 1}}}`.

Create some indexes:

    > curl "http://localhost:8080/index?col=Feeds&path=a,b,c"
    > curl "http://localhost:8080/index?col=Feeds&path=Title"
    > curl "http://localhost:8080/index?col=Feeds&path=Source"
    > curl "http://localhost:8080/index?col=Feeds&path=Age"

What indexes do I have now?

    > curl "http://localhost:8080/indexes?col=Feeds"
    ["a,b,c","Title","Source","Age"]

Remove an index:

    > curl "http://localhost:8080/unindex?col=Feeds&path=a,b,c"

## Queries

Prepare some documents:

    > curl --data-ascii doc='{"Title": "New Go release", "Source": "golang.org", "Age": 3}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Kitkat is here", "Source": "android.com", "Age": 2}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Slackware Beta", "Source": "slackware.com", "Age": 1}' "http://localhost:8080/insert?col=Feeds"

Looking for the article "New Go Release":

    > curl --data-ascii q='{"eq": "New Go release", "in": ["Title"]}' "http://localhost:8080/query?col=Feeds"
    [{"Age":3,"Source":"golang.org","Title":"New Go release","@id":"10230803398370864725"}]

Looking for article "New Go release" and "android.com" feeds:

    > curl --data-ascii q='[{"eq": "New Go release", "in": ["Title"]}, {"eq": "android.com", "in": ["Source"]}]' "http://localhost:8080/query?col=Feeds"
    [{"Age":3,"Source":"golang.org","Title":"New Go release","@id":"10230803398370864725"},{"Age":2,"Source":"android.com","Title":"Kitkat is here","@id":"8602039814711744373"}]

Looking for all but not feeds from "golang.org":

    > curl --data-ascii q='{"c": [{"eq": "golang.org", "in": ["Source"]}, "all"]}' "http://localhost:8080/query?col=Feeds"
    [{"Age":2,"Source":"android.com","Title":"Kitkat is here","@id":"8602039814711744373"},{"Age":1,"Source":"slackware.com","Title":"Slackware Beta","@id":"12333034197694914883"}]

Note that: `"all"` means "all documents"; `{"c": [ .. ]}` means "complement". 

Looking for young feeds (age between 1 and 3) from "slackware.com":

    > curl --data-ascii q='{"n": [{"eq": "slackware.com", "in": ["Source"]}, {"int-from": 1, "int-to": 3, "in": ["Age"]}]}' "http://localhost:8080/query?col=Feeds"
    [{"Age":1,"Source":"slackware.com","Title":"Slackware Beta","@id":"12333034197694914883"}]

`{"n": [ .. ]}` means "intersect"; `{"int-from": x, "int-to": y, "in": [ .. ] }` is an integer range lookup.

There is also `count` API to return number of results.

## Wrap up

Let's gracefully shutdown server:

    > curl "http://localhost:8080/shutdown"