# Embedded Usage

## Introduction

tiedot is designed for both running standalone service and embedded usage.

Normally, when you run tiedot as standalone service, tiedot would be managing `GOMAXPROCS`. However, if used as an embedded database, you will need to manage `GOMAXPROCS` to ensure maximized performance.

My experiments usually show best performance outcome when `GOMAXPROCS` is set to 2 * number of CPUs - this is also the tiedot default settings.

## Usage

APIs for embedded usage have been demonstrated in `example.go`, you may run the example by building tiedot and run:

    ./tiedot -mode=example