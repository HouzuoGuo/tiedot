#!/bin/sh

go fmt && go build
export TIEDOT_EXEC=`pwd`/tiedot
cd server
go fmt
go test -run=TestColMgmt
