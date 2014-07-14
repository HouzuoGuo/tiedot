### Directory and file structure

<pre>
TiedotDatabase         # A database "TiedotDatabase"
├── CollectionA        # A collection called "CollectionA"
│   ├── Book!Author!Name   # An index on path "Book" -> "Author" -> "Name"
│   │   ├── 0                  # Index data partition 0
│   │   └── 1                  # Index data partition 1
│   ├── dat_0              # Document data partition 0
│   ├── dat_1              # Document data partition 1
│   ├── id_0               # Document ID lookup table for partition 0
│   └── id_1               # Document ID lookup table for partition 1
├── CollectionB        # Another collection called "CollectionB"
│   ├── Day!Temperature!High
│   │   ├── 0
│   │   └── 1
│   ├── dat_0
│   ├── dat_1
│   ├── id_0
│   └── id_1
└── number_of_partitions
</pre>

### Data file structure

Collection data file contains document data. Every document has a binary header and UTF-8 text content. The file has an initial size (32MB) and will grow beyond the initial size (by 32MB incrementally) to fit more documents.

New documents are inserted to end-of-data position, and they are left with room for future updates and size growth. Every document is assigned to a randomly generated, practically unique document ID, which also decides into which partition the document goes.

Updating document usually happens in-place, however if there is not pre-allocated enough room for the updated version, the document has to be deleted and re-inserted; document ID remains the same.

Deleted documents are marked as deleted, the wasted space is recovered in the next scrub operation.

#### Document format on disk

<table>
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Byte (8 bit signed integer)</td>
    <td>1</td>
    <td>Validity</td>
    <td>0 - deleted, 1 - valid</td>
  </tr>
  <tr>
    <td>Signed 64-bit integer</td>
    <td>10</td>
    <td>Allocated room</td>
    <td>How much room is left for the document</td>
  </tr>
  <tr>
    <td>Char Array</td>
    <td>Size of document content</td>
    <td>Document content</td>
    <td>Encoded in UTF-8</td>
  </tr>
  <tr>
    <td>Char Array</td>
    <td>Allocated room - size of document</td>
    <td>Padding (UTF-8 spaces)</td>
    <td>Room for future updates, for the document to grow its size</td>
  </tr>
</table>

### Index hash table file structure

Hash table file contains binary content; it implements a static hash table made of hash buckets and integer entries.

Every bucket has a fixed number of entries. When a bucket becomes full, a new bucket is chained to it in order to store more entries. Every entry has an integer key and value.

An entry key may have multiple values assigned to it, however the combination of entry key and value must be unique
across the entire hash table.

#### Bucket format on disk

<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Signed 64-bit integer</td>
    <td>10</td>
    <td>Next chained bucket number</td>
    <td>When a bucket is the last in its chain, this number is 0.</td>
  </tr>
  <tr>
    <td>Bucket Entry</td>
    <td>21 * number of entries per bucket</td>
    <td>Bucket entries</td>
    <td>See "Bucket entry format"</td>
  </tr>
</table>

#### Bucket entry format

<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Byte (signed 8-bit integer)</td>
    <td>1</td>
    <td>Validity</td>
    <td>0 - deleted, 1 - valid</td>
  </tr>
  <tr>
    <td>Signed 64-bit integer</td>
    <td>10</td>
    <td>Key</td>
    <td>Entry key</td>
  </tr>
  <tr>
    <td>Signed 64-bit integer</td>
    <td>10</td>
    <td>Value</td>
    <td>Entry value</td>
  </tr>
</table>