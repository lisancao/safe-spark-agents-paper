---
name: pyspark-sdp
description: Mechanics of the Open-Source Spark Declarative Pipelines API (OSS SDP) on pyspark 4.1 — how to write/declare/run a pipeline that passes `pipelines/cli.py dry-run`. NOT Databricks DLT.
---

# pyspark-sdp — Open-Source Spark Declarative Pipelines API (pyspark 4.1)

This pack teaches ONLY the **API mechanics** of the **open-source** Apache Spark
`pipelines` framework (`pyspark 4.1`) — how to declare datasets, acquire the
session, reference upstream tables, write a valid spec, and run the dry-run gate.
It is NOT Databricks Delta Live Tables, and it says nothing about what your
pipeline should compute — that is your job. Every API fact below is verified live
against `pyspark 4.1` `pipelines/cli.py dry-run`; follow it exactly rather than
improvising from memory of DLT.

## The most common failure: do NOT use Databricks DLT

WRONG (Databricks-only; the module does not exist here — `ModuleNotFoundError`):

```python
import dlt                       # WRONG — no such module on this cluster
@dlt.table                       # WRONG
def my_view(): ...
dlt.read("upstream")             # WRONG
dlt.read_stream("upstream")      # WRONG
```

RIGHT (OSS Spark Declarative Pipelines):

```python
from pyspark import pipelines as dp        # the OSS decorator namespace
from pyspark.sql import SparkSession
```

## Decorators: `@dp.materialized_view` for BATCH, `@dp.table` is STREAMING-only

Declare a **batch** dataset (one whose query is a batch relation —
`spark.read.*`, `spark.range`, etc.) with `@dp.materialized_view`:

```python
@dp.materialized_view(name="my_dataset")
def my_dataset():
    return SparkSession.active().range(3)
```

`@dp.table` defines a **STREAMING TABLE** and **REJECTS a batch relation** at
dry-run:

```
[INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE]
```

So a plain batch query (`spark.read.table/json/text`, `spark.range`) MUST use
`@dp.materialized_view`, never `@dp.table`. Use `@dp.table` only when the source is
genuinely a stream (`spark.readStream...`). Both decorator call forms are verified:
`@dp.materialized_view` and `@dp.materialized_view(name="...")`. Prefer the explicit
`name=` so the published table name is deterministic.

## Getting the session — there is NO `dp.get_spark`/`dp.read`

```python
spark = SparkSession.active()              # verified
# (SparkSession.getActiveSession() is the equivalent older spelling)
```

These do **NOT exist** — calling them is an `AttributeError`:

- `dp.get_spark()`
- `dp.current_spark_session()`
- `dp.read(...)` / `dp.read_table(...)` / `dp.readStream(...)`

## Referencing another dataset in the pipeline

To read a dataset defined elsewhere in THIS pipeline, use the ordinary Spark
reader against its declared name:

```python
@dp.materialized_view(name="downstream")
def downstream():
    return SparkSession.active().read.table("my_dataset").select("id")
```

If a bare name raises `[TABLE_OR_VIEW_NOT_FOUND]` on this build, qualify it with
the spec's catalog/database and re-run dry-run:

```python
SparkSession.active().read.table("spark_catalog.default.my_dataset")
```

Keep dependency chains simple and dry-run-verified; deep multi-hop references are
the main thing that trips name resolution on this build.

## Build UNRESOLVED plans only — never force analysis/execution

A query function must return a DataFrame whose plan is **unresolved**. Do NOT call
anything inside the function body that forces analysis or execution:

- NO `.collect()`, `.count()`, `.show()`, `.first()`, `.take()`, `.toPandas()`
- NO `spark.createDataFrame(...)` (forces local analysis)

Any of these raises:

```
[ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION]
```

Use only lazy, plan-building operations: `spark.range`, `spark.read.table/json/
text`, `select`, `withColumn`, `where`/`filter`, `groupBy().agg()`, `join`, and
column expressions/functions. These build a plan without forcing analysis.

## Spec file (`spark-pipeline.yml`) requirements

These rules make the dry-run pass:

- `storage:` MUST be a **URI** — `file:///abs/path` locally or `s3a://...` on a
  cluster. A bare path is rejected.
- Library glob MUST be `transformations/**`. `transformations/*.py` is **rejected**.
- `catalog:` and `database:` MUST be present (omitting them yields
  `PARSE_EMPTY_STATEMENT`). `spark_catalog` / `default` are proven-good.

```yaml
name: my_pipeline
storage: file:///tmp/pipeline-storage     # or s3a://.../warehouse
catalog: spark_catalog
database: default
libraries:
  - glob:
      include: transformations/**
```

The transform module(s) live under `transformations/` (matched by the glob).

## Generic two-dataset example (mechanics only)

A minimal, NON-task pipeline showing the shape: one source materialized view and
one downstream view that reads it. Use this as the API template; what you actually
compute is up to you.

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

@dp.materialized_view(name="bronze")
def bronze():
    return SparkSession.active().range(3)          # unresolved batch plan

@dp.materialized_view(name="silver")
def silver():
    src = SparkSession.active().read.table("bronze")
    return src.select((F.col("id") * 2).alias("doubled"))   # still unresolved
```

## Dry-run-first workflow

1. Write the transform module(s) under `transformations/`.
2. Run the structural gate: `pipelines/cli.py dry-run --spec spark-pipeline.yml`
   (driver-only, ~8s, $0 — no executors, no data movement).
3. If it fails, read the `[ERROR_CLASS]`. Common ones:
   - `BATCH_RELATION_FOR_STREAMING_TABLE` — used `@dp.table` for a batch query;
     switch to `@dp.materialized_view`.
   - `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION` — called an action; remove it.
   - `TABLE_OR_VIEW_NOT_FOUND` — qualify the name with `spark_catalog.default.<n>`.
   - `PARSE_EMPTY_STATEMENT` — spec missing `catalog`/`database`.
   Fix and re-dry-run until clean before any `run` (`cli.py run --spec ...`).
