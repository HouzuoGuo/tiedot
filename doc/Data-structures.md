### Directory and file structure

<pre>
TiedotDatabase         # Database "TiedotDatabase
|-- CollectionA            # Collection "CollectionA"
|   |-- chunk_0                # Partition 0
|   |   |-- _data                  # Per-partition document data
|   |   `-- _pk                    # Per-partition primary index
|   |-- chunk_1                # Partiton 1
|   |   |-- _data                  # Per-partition document data
|   |   `-- _pk                    # Per-partition primary index
|   |-- ht_Book,Author,Name    # Secondary index on ["Book, "Author", "Name"]
|   |   |-- 0                      # Index partition 0
|   |   `-- 1                      # Index partition 1
|   `-- numchunks              # Text file - content indicates number of partitions
`-- CollectionB
    |-- chunk_0
    |   |-- _data
    |   `-- _pk
    |-- chunk_1
    |   |-- _data
    |   `-- _pk
    |-- chunk_2
    |   |-- _data
    |   `-- _pk
    |-- ht_Day,Temperature,High
    |   |-- 0
    |   |-- 1
    |   `-- 2
    `-- numchunks
</pre>

### Data file structure

Every document has a random and practically unique ID that is also the primary index value - it decides in which partition the document goes into.

File has a capacity and may grow beyond the capacity to fit more documents. New documents are inserted to end-of-data position, and they are left room for future updates and size growth.

Updating document usually happens in-place, however if there is not enough room for the updated version, the document has to be deleted and re-inserted. Deleted documents are marked as deleted.

Document partition is initially 16MB. It grows automatically by 16MB when there is no place left to append more documents.

#### Document format

There is no padding before or after a document. Every document has:
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
    <td>Unsigned 64-bit integer</td>
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

All indexes (primary and secondary) are made of static hash tables of buckets. All buckets have the same number of entries and new buckets will be chained together should a bucket grow full.

#### Bucket format

There is no padding before or after a bucket. Each bucket is stored in the following format:
<table style="width: 100%;">
  <tr>
    <th>Type</th>
    <th>Size (bytes)</th>
    <th>Description</th>
    <th></th>
  </tr>
  <tr>
    <td>Unsigned 64-bit integer</td>
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
There is no padding before or after an entry. Each entry is stored in the following format:
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
    <td>Unsigned 64-bit integer</td>
    <td>10</td>
    <td>Key</td>
    <td>Entry key</td>
  </tr>
  <tr>
    <td>Unsigned 64-bit integer</td>
    <td>10</td>
    <td>Value</td>
    <td>Entry value</td>
  </tr>
</table>