#!/usr/bin/env bash

set -e
echo '' > coverage.txt

for dir in $(go list ./... | grep -v vendor | grep -v examples | grep -v benchmark); do
    go test -race -coverprofile=pkgcoverage.txt -covermode=atomic "$dir"
    if [ -f pkgcoverage.txt ]; then
        cat pkgcoverage.txt >> coverage.txt
        rm pkgcoverage.txt
    fi
done
