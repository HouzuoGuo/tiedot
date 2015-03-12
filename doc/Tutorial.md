# Tutorial: tiedot in 10 minutes

You will need:

- Git
- Go (>= 1.1)
- A regular web browser OR curl/wget

## Run tiedot

    mkdir tiedot && cd tiedot
    export GOPATH=`pwd`  # backticks surround pwd
    go get github.com/HouzuoGuo/tiedot

    ./bin/tiedot -mode=httpd -dir=/tmp/MyDatabase -port=8080

## Basics

tiedot HTTP server serves one database; collections in a database are automatically partitioned; the collections may use indexes to run queries.

Each new document has an automatically assigned unique ID (string) that will always stay with the document.

API requests (along with parameter values) may be sent using GET, POST or PUT HTTP method.

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

## Manage documents

Insert document:

    > curl --data-ascii doc='{"a": 1, "b": 2}' "http://localhost:8080/insert?col=Feeds"
    791206372389179361 # the new document's unique ID

Read document:

    > curl "http://localhost:8080/get?col=Feeds&id=791206372389179361"
    {"a":1,"b":2}

Update document:

    > curl --data-ascii doc='{"a": 3, "b": 4}' "http://localhost:8080/update?col=Feeds&id=791206372389179361"

Delete document:

    > curl "http://localhost:8080/delete?col=Feeds&id=791206372389179361"

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
    ["a","b","c"],["Title"],["Source"],["Age"]]

Remove an index:

    > curl "http://localhost:8080/unindex?col=Feeds&path=a,b,c"

## Queries

Prepare some documents:

    > curl --data-ascii doc='{"Title": "New Go release", "Source": "golang.org", "Age": 3}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Kitkat is here", "Source": "android.com", "Age": 2}' "http://localhost:8080/insert?col=Feeds"
    > curl --data-ascii doc='{"Title": "Slackware Beta", "Source": "slackware.com", "Age": 1}' "http://localhost:8080/insert?col=Feeds"

Looking for the article "New Go Release":

    > curl --data-ascii q='{"eq": "New Go release", "in": ["Title"]}' "http://localhost:8080/query?col=Feeds"
    {"356740846970476516":{"Age":3,"Source":"golang.org","Title":"New Go release"}}

Looking for article "New Go release" and "android.com" feeds:

    > curl --data-ascii q='[{"eq": "New Go release", "in": ["Title"]}, {"eq": "android.com", "in": ["Source"]}]' "http://localhost:8080/query?col=Feeds"
    {"356740846970476516":{"Age":3,"Source":"golang.org","Title":"New Go release"},"8835531386221862775":{"Age":2,"Source":"android.com","Title":"Kitkat is here"}}

Looking for all but not feeds from "golang.org":

    > curl --data-ascii q='{"c": [{"eq": "golang.org", "in": ["Source"]}, "all"]}' "http://localhost:8080/query?col=Feeds"
    {"4530407170288349686":{"Age":1,"Source":"slackware.com","Title":"Slackware Beta"},"8835531386221862775":{"Age":2,"Source":"android.com","Title":"Kitkat is here"}}

Note that: `"all"` means "all documents"; `{"c": [ .. ]}` means "complement". 

Looking for young feeds (age between 1 and 3) from "slackware.com":

    > curl --data-ascii q='{"n": [{"eq": "slackware.com", "in": ["Source"]}, {"int-from": 1, "int-to": 3, "in": ["Age"]}]}' "http://localhost:8080/query?col=Feeds"
    [{"Age":1,"Source":"slackware.com","Title":"Slackware Beta","@id":"12333034197694914883"}]

`{"n": [ .. ]}` means "intersect"; `{"int-from": x, "int-to": y, "in": [ .. ] }` is an integer range lookup.

There is also `/count?col=foo` API to return number of results.

## Wrap up

Let's gracefully shutdown server:

    > curl "http://localhost:8080/shutdown"
