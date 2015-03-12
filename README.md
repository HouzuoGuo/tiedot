Keywords: Golang, go, document database, NoSQL, JSON

<img src="http://golang.org/doc/gopher/frontpage.png" alt="Golang logo" align="right"/>

### tiedot - Your NoSQL database powered by Golang

[![Build Status](https://travis-ci.org/HouzuoGuo/tiedot.svg?branch=master)](https://travis-ci.org/HouzuoGuo/tiedot) [![GoDoc](https://godoc.org/github.com/HouzuoGuo/tiedot?status.svg)](https://godoc.org/github.com/HouzuoGuo/tiedot) [Open Build Service](https://build.opensuse.org/package/show/home:guohouzuo/tiedot)

tiedot is a document database engine that uses __JSON__ as document notation; it has a powerful query processor that supports advanced set operations; it can be __embedded__ into your program, or run a stand-alone server using __HTTP__ for an API. It runs on *nix and Windows operating systems.

tiedot has fault-tolerant data structures that put your data safety *first*, while easily scales to 4+ CPU cores.

> tiedot has very stable performance, even with millions of records! It consistently achieves high throughput - swallow more than 120k records or 80k complicated queries per second with confidence.

### Contributions welcome!

tiedot is a very small project in the large open source community - it is growing fast thanks to the 800+ stars and watchers, as well as many contributors for their feedback, comments, ideas and code. Your contribution matters a lot!

Pull requests/forks all welcome, and please share your thoughts, questions and feature requests in [Issues] section.

Let me know what you think about tiedot, I love to hear from you! Please [Email me], follow my [Twitter] and [blog].

### References

- [Tutorial: tiedot in 10 minutes]
- [API reference and embedded usage]
- [Limitations]

... and more

- [Performance tuning and benchmarks]
- [Data structures]
- [Query processor and index]
- [Concurrency and networking]

### Get tiedot!

tiedot is distributed under the [Simplified BSD license][Contributors and License].

The newest version 3.2 introduces Javascript Web Token authorization in addition to the conventional HTTP APIs. Please check out [Version History] for change logs and more version information.

### Project Story

Go is a fascinating language and it has attracted a lot of attention in the open source community over the past few years. But as a young programming language, there was a lack of database engine options for back-end development. As we all know, the design of Go was heavily inspired by C language, and there is sqlite for C - so I thought what about a making DB engine easily embeddable for Go programs?

Then tiedot was born - the name "tiedot" is a cute Finnish word meaning "data". Initially being a programming exercise, tiedot greatly helped me exploring the performance characteristics of Go.

Thanks to the very welcoming and helpful community of Go - when tiedot made its first release, a lot of people contributed feature ideas and suggestions pushing the project well beyond its initial goal. Till this day tiedot has been used in data analysis, embedded systems, utility applications, web applications, and some people also find it useful being a demo of several DB engine mechanisms.

I love Go because it offers stable and predictable performance characteristics, very fast compilation speed and a complete toolchain for all development activities. And unlike many other languages, Go does not require an intelligent IDE to improve productivity of users - many of which find a syntax-highlighted text editor to be sufficient for coding Go!

There are several other data structure servers/DB engines written in Go, namely "etcd", "leveldb-go" and "ql"; they emphasis on different usage scenarios, for example "etcd" is a data structure server designed for configuration management and service discovery, "leveldb-go" is a re-implementation of popular LevelDB in Go, and "ql" is an embeddable DB engine providing SQL capability. After all, when it comes to choosing a general purpose, embeddable NoSQL database engine, tiedot is perhaps the only choice so far (as far as I know).

[Tutorial: tiedot in 10 minutes]: https://github.com/HouzuoGuo/tiedot/wiki/Tutorial
[API reference and embedded usage]: https://github.com/HouzuoGuo/tiedot/wiki/API-reference-and-embedded-usage
[Version History]: https://github.com/HouzuoGuo/tiedot/wiki/Version-History
[Data structures]: https://github.com/HouzuoGuo/tiedot/wiki/Data-structures
[Query processor and index]: https://github.com/HouzuoGuo/tiedot/wiki/Query-processor-and-index
[Concurrency and networking]: https://github.com/HouzuoGuo/tiedot/wiki/Concurrency-and-networking
[Performance tuning and benchmarks]: https://github.com/HouzuoGuo/tiedot/wiki/Performance-tuning-and-benchmarks
[Limitations]: https://github.com/HouzuoGuo/tiedot/wiki/Limitations
[Email me]: mailto:guohouzuo@gmail.com
[Twitter]: https://twitter.com/hzguo
[blog]: http://allstarnix.blogspot.com.au
[Issues]: https://github.com/HouzuoGuo/tiedot/issues
[Contributors and License]: https://github.com/HouzuoGuo/tiedot/wiki/Contributors-and-License