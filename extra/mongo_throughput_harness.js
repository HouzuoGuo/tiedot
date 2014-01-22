// Mongo throughput benchmark harness.
// The entire setup corresponds to tiedot bench(1).

// Prepare a collection with 2 indexes
use bench;
db.dropDatabase()
db.createCollection("col")
db.col.ensureIndex({"a": 1})
db.col.ensureIndex({"b": 1})

// Insert approx. half million documents, 1KB each
ops = [{op: "insert", ns: "bench.col", safe: false, doc: {"a": {"#RAND_INT": [0, 1000000]}, "b": {"#RAND_INT": [0, 1000000]}, "more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}}]
benchRun({parallel: 8, seconds: 50, ops: ops})

// Read: index lookup of one attribute
ops = [{op: "findOne", ns: "bench.col", query: {"a": {"#RAND_INT": [0, 1000000]}}}]
benchRun({parallel: 8, seconds: 10, ops: ops})

// Query: index lookup on two document attributes
ops = [{op: "findOne", ns: "bench.col", query: {"a": {"#RAND_INT": [0, 1000000]}, "b": {"#RAND_INT": [0, 1000000]}}}]
benchRun({parallel: 8, seconds: 10, ops: ops})

// Update that rewrites both indexed attributes
ops = [{op: "update", ns: "bench.col", safe: false, query: {"a": {"#RAND_INT": [0, 1000000]}}, update: {"a": "updated", "b": "updated"}}]
benchRun({parallel: 8, seconds: 10, ops: ops})

// Delete
db.col.count()
ops = [{op: "remove", ns: "bench.col", query: {"a": {"#RAND_INT": [0, 1000000]}}}]
benchRun({parallel: 8, seconds: 10, ops: ops})
db.col.count()