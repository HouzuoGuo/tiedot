Keywords: Golang, go, document database, NoSQL, JSON

<img src="http://golang.org/doc/gopher/frontpage.png" alt="Golang logo" align="right"/>

### tiedot - Your NoSQL database powered by Golang

tiedot is a document database engine that uses __JSON__ as document notation; it has a powerful query processor that supports advanced set operations; it can be __embedded__ into your program, or run a stand-alone server using __HTTP__ for an API on most *nix and Windows operating systems.

tiedot has fault-tolerant data structures that put your data safety *first*, while considering scalability in design.

> Did you know? The largest known tiedot deployment powers a personal offline Wikipedia indexing project, it has 5.9 million documents and over 73GB of data.

### Contributions welcome!

tiedot is a very small project in the large open source community - it is growing fast thanks to the 500+ stars and watchers, as well as many contributors for their feedback, comments, ideas and code. Your contribution matters a lot!

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

Please clone branch 2.1 for the latest features. Please check out [Version History] for change logs and historical version informaion.

### Project Story

__Is tiedot "yet another NoSQL database"?__

There are probably as many NoSQL database as there are Linux distributions.

tiedot is not as powerful (yet) - and does not intend to compete with mainstream NoSQL database engines such as CouchDB or Cassandra. However, tiedot performs reasonably well given its small size (around 3k LOC); and due to its simplicity, its performance may come close or even exceed those large brand NoSQL solutions (under certain workloads).

__What is the motive behind this project?__

Golang (Go) is a fascinating language - very easy to use, scalable and reasonably stable. I am very passionate about document database technologies (check out my other GitHub projects!) and enjoy seeing my code scaling well on SMP machines. This is my Golang exercise.

__Why the name "tiedot"?__

"tiedot" is a Finnish word standing for "data". I enjoy learning (natural and computer) languages, also enjoy listening to music in many languages. "Tiedot" sounds cute, doesn't it?

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
