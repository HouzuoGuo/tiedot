# Data structures

## Directory and file structure

Database is an ordinary directory in file system, tiedot requires to have RWX permissions on the directory.

Collection is a directory under Database directory, the directory is named using the collection's name. tiedot requires to have RWX permissions on the directory.

Collection has the following files:

- `data` documents data
- `config` index configuration
- `config.bak` old index configuration (may not exist)

It may also have several index files - one for each index.

## Data file structure

All documents are stored in data file. New documents are inserted to end-of-data* position, and they are left room for future updates (growth).

Updating document usually happens in-place, however if there is not enough room for the updated version, the document has to be deleted and re-inserted.

Deleted documents are marked as deleted.

Data file is initially 128MB. It grows automatically by 128MB when there is no place left to append more documents.

### Document format

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

## Index hash table file structure

This is a static hash table made of buckets. All buckets have same number of entries, new buckets will be chained together should a bucket becomes full.

Index file automatically grows by 64MB when there is no place left for more buckets.

### Bucket format

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
    <td>22 * number of entries per bucket</td>
    <td>Bucket entries</td>
    <td>See "Bucket entry format"</td>
  </tr>
</table>

### Bucket entry format
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