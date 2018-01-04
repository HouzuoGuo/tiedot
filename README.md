
<h1 align="center">tiedot</h1>
<p align="center">
<a href="https://travis-ci.org/HouzuoGuo/tiedot.svg?branch=master"><img src="https://travis-ci.org/HouzuoGuo/tiedot.svg?branch=master" alt="Build Status"></a>
    <a href="https://codecov.io/gh/HouzuoGuo/tiedot"><img src="https://codecov.io/gh/HouzuoGuo/tiedot/branch/master/graph/badge.svg" alt="codecov"></a>
      <a href="https://godoc.org/github.com/HouzuoGuo/tiedot"><img src="https://godoc.org/github.com/HouzuoGuo/tiedot?status.svg" alt="GoDoc"></a> 
 </p>
 
<p align="center"> <a href="http://tiedot.github.io"><strong>Documentation</strong></a> </p>

Keywords: Golang, go, document database, NoSQL, JSON

<img src="http://golang.org/doc/gopher/frontpage.png" alt="Golang logo" align="right"/>

### tiedot - Your NoSQL database powered by Golang

tiedot is a document database engine that uses __JSON__ as document notation; it has a powerful query processor that supports advanced set operations; it can be __embedded__ into your program, or run a stand-alone server using __HTTP__ for an API. It runs on *nix and Windows operating systems.

tiedot has fault-tolerant data structures that put your data safety *first*, while easily scales to 4+ CPU cores.

> tiedot has very stable performance, even with millions of records! It consistently achieves high throughput - swallow more than 120k records or 80k complicated queries per second with confidence.

### Get tiedot!

tiedot is distributed under the [Simplified BSD license][Contributors and License].

The newest version 3.4 comes with general performance and compatibility improvements. Find out more in [releases](https://github.com/HouzuoGuo/tiedot/releases).

### Running in Docker
Run tiedot with help from [docker](https://docs.docker.com/engine/installation/) and [docker compose](https://docs.docker.com/compose/install/):

    $ docker-compose build
    $ docker-compose up -d

To view the logs:

    $ docker-compose logs

### References
- [Tutorial: tiedot in 10 minutes]
- [API reference and embedded usage]
- [Limitations]

... and more

- [Performance tuning and benchmarks]
- [Data structures]
- [Query processor and index]
- [Concurrency and networking]

### Contributions welcome!
tiedot is a very small project in the large open source community - it is growing fast thanks to the 800+ stars and watchers, as well as many contributors for their feedback, comments, ideas and code. Your contribution matters a lot!

Pull requests/forks all welcome, and please share your thoughts, questions and feature requests in [Issues] section.

Let me know what you think about tiedot, I'd love to hear from you! Please [Email me], follow my [Twitter] and [blog].

The Go gopher was designed by Renee French. (http://reneefrench.blogspot.com/).
The Go gopher is covered by the [Creative Commons Attribution 3.0][Creative Commons Attribution 3.0] license.

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
[Creative Commons Attribution 3.0]: http://creativecommons.org/licenses/by/3.0


