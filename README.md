# Holocene

`holocene` is a follow up to [`eocene`](https://github.com/clflushopt/eocene) where we implement
a vectorized, push based query engine using Arrow as the data format.

## Push-based Vectorized Execution

Vectorized execution in the context of database workloads means **batches of records**, most
often when speaking about _vectorized execution_ the meaning is along the lines of the _Volcano_
model, but instead of `next()` returning a single record, `next()` returns multiple records.

Actual _vectorization_ as in, SIMD instructions, is sometimes used to implement faster
compute kernels but they don't mean the entire query plan is vectorized, but the plan
can indeed be executed in parallel.

Push-based in this context describes a paradigm different from the Volcano model , where operators
push their results down the pipeline. This approach has the benefit that the query plan becomes
a DAG that can be executed in parallel, except for pipeline breakers that can be seen as join
points.

_Vectorized + push-based models_ are extremely [good for OLAP workloads](https://15721.courses.cs.cmu.edu/spring2024/slides/05-execution2.pdf)
and represent the union of two ideas, push-based models and vectorized models.

```
Parallelizable part of the pipeline
each step pushes, multiple records
down the pipeline
                                                       Pipeline breaking, since LIMIT
                                                       will be applied over all records
  +--------+                                                             |
  | Batch  |    +--------+    +------+    +--------+    +------------+   |    +-------+
  +--------+--->| Source |--->| Scan |--->| Filter |--->| Projection |   |    |       |
                +--------+    +------+    +--------+    +------------+   |    |       |
  +--------+                                                             |    |       |
  | Batch  |    +--------+    +------+    +--------+    +------------+   |    |       |
  +--------+--->| Source |--->| Scan |--->| Filter |--->| Projection |---+--->| Limit |
                +--------+    +------+    +--------+    +------------+   |    |       |
  +--------+                                                             |    |       |
  | Batch  |    +--------+    +------+    +--------+    +------------+   |    |       |
  +--------+--->| Source |--->| Scan |--->| Filter |--->| Projection |   |    |       |
                +--------+    +------+    +--------+    +------------+   |    +-------+
                                                                         |
  +--------+                                                             |
  | Batch  |                                                             |
  +--------+                                                             |

```
