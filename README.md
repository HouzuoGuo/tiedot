<img src="http://golang.org/doc/gopher/frontpage.png" alt="Golang logo" align="right"/>

tiedot - Your NoSQL database powered by Golang
=

tiedot is a document database that uses __JSON__ for documents and queries; it can be __embedded__ into your program, or run a stand-alone server using __HTTP__ for an API.

Feature Highlights
-
- Designed for both embedded usage and standalone service.
- Fault-tolerant data structures that put safety of your data *first*.
- Built with performance and scalability always in mind.
- Use JSON syntax to build powerful queries.

High Performance!
-
tiedot scales reasonably well on SMP machines. Under maximum load, it usually either uses all CPUs to 100%, or uses up all IO bandwidth. The following performance results are collected on three different types of machines, using tiedot built-in benchmark:

(Operations per second)
<table>
<tr>
  <th>Processor</th>
  <th>Insert</th>
  <th>Read</th>
  <th>Query</th>
  <th>Update</th>
  <th>Delete</th>
  <th></th>
</tr>
<tr>
  <td>Mobile Intel Core i7 (2nd Gen)</td>
  <td>140k</td>
  <td>310k</td>
  <td>58k</td>
  <td>60k</td>
  <td>140k</td>
  <td>A 3 years old laptop</td>
</tr>
<tr>
  <td>Desktop Intel Core i2</td>
  <td>107k</td>
  <td>231k</td>
  <td>44k</td>
  <td>48k</td>
  <td>90k</td>
  <td>A 5 years old workstation</td>
</tr>
<tr>
  <td>Amazon EC2 m1.xlarge</td>
  <td>90k</td>
  <td>188k</td>
  <td>39k</td>
  <td>42k</td>
  <td>116k</td>
  <td>Medium range instance type</td>
</tr>
<tr>
  <td>Amazon EC2 t1.micro</td>
  <td>18k</td>
  <td>70k</td>
  <td>15k</td>
  <td>19k</td>
  <td>54k</td>
  <td>The slowest instance type</td>
</tr>
</table>

References
-
- [Quick Start Guide: tiedot in 10 minutes][tutorial]
- [Embedded usage]
- [Data structures]
- [Query processor and index]
- [Concurrency and networking]
- [Limitations]

Version History
-
<table>
<tr>
  <th>Version</th>
  <th>Branch</th>
  <th>Release Date</th>
  <th>Comment</th>
</tr>
<tr>
  <td>Alpha</td>
  <td>alpha</td>
  <td>2013-07-01</td>
  <td>Initial release</td>
</tr>
</table>

Contact and License
-

You may want to check out [Issues] section for future plans, and please feel very free to contact [Howard] if you have any feedback / questions. I also have [Twitter] and [blog], please check them out as well.

The following copyright notice and disclaimers apply to all files in the project repository:
<pre>
Copyright (c) 2013, Howard Guo
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
- Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
</pre>


Project Background
-
__Is tiedot "yet another NoSQL database"?__

There are probably as many NoSQL database as there are Linux distributions.

tiedot is not as powerful (yet) - and does not intend to compete with mainstream NoSQL database engines such as CouchDB or Cassandra. However, tiedot performs reasonably well given its small size (< 3k LOC); and for certain loads, it may perform as well as those large brand NoSQL solutions.

__What is the motive behind this project?__

Golang (Go) is a fascinating language - very easy to use, scalable and reasonably stable. I am very passionate about document database technologies (check out my other GitHub projects!) and enjoy the moments when my program scales well on SMP machines. So this is my Golang practice.

__Why the name "tiedot"?__

"tiedot" is a Finnish word standing for "data". I enjoy learning (natural and computer) languages, also enjoy listening to music in many languages. "Tiedot" sounds cute, doesn't it?

[tutorial]:
[Howard]: mailto:guohouzuo@gmail.com
[Twitter]: https://twitter.com/hzguo
[blog]: http://allstarnix.blogspot.com.au
[Issues]: https://github.com/HouzuoGuo/tiedot/issues
