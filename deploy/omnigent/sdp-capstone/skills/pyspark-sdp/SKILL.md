---
name: pyspark-sdp
description: How to author correct OSS Spark Declarative Pipelines (SDP) medallions for the custodian.
---

# pyspark-sdp: authoring OSS Spark Declarative Pipelines

This is the shared, governed skill for the SDP capstone fleet. Every sub-agent that
authors or reviews a pipeline uses it, so fleet competence is a property of the
orchestrator, not luck per session.

## What SDP is (and is not)
OSS SDP is `pyspark.pipelines` on pyspark 4.1. It is NOT Databricks DLT: never
`import dlt`, never `@dlt`. Import the API as `from pyspark import pipelines as dp`.

## The contract the custodian executes
- Declare each dataset with `@dp.materialized_view` on a function that RETURNS a
  DataFrame. The function name is the dataset name.
- A live `spark` session is provided, plus `from pyspark.sql import functions as F`
  and `from pyspark.sql import Window`. Do NOT open or getOrCreate a SparkSession,
  and do NOT call `.write` / `.saveAsTable` / `.start`.
- Read an upstream dataset with `spark.read.table('name')`, using the EXACT dataset
  names in the brief so the DAG wires up (bronze reads the raw table; silver reads
  bronze; gold reads silver).
- The custodian materializes each dataset over the customer's own tenant and returns
  pass/fail plus per-dataset errors and the contextual policy result.

## The correctness rules that matter (these are what fail pipelines)
- ANSI mode is ON. A plain `.cast("double")` on a bad string like `'$30.00'` or
  `'abc'` THROWS. Strip non-numeric characters then use `try_cast`, e.g.
  `F.expr("try_cast(regexp_replace(amount, '[$,]', '') as double)")`. `F.try_cast`
  may be unavailable in the client; prefer `F.expr`.
- Dedup on a real ordering key, not a raw string timestamp. Parse the timestamp,
  keep the latest per key with a `Window` + `row_number`.
- Corrupt or unknown rows must be QUARANTINED into a separate dataset (a name
  containing `quarantine`), never silently dropped.
- UTC day buckets: derive the date from a parsed timestamp in UTC, not the session
  timezone.
- PII: when a policy marks a column PII (e.g. region), it must NOT appear as a raw
  cleartext column in any gold table. Tokenize it (a `sha2` hash column) or bucket it.
- Value conservation: for financial data, every input row must land in either the
  clean output or the quarantine, so `count(raw) == count(silver) + count(quarantine)`.

## Shape of a medallion (illustrative)
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

@dp.materialized_view
def bronze_x():
    return spark.read.table("raw_x")

@dp.materialized_view
def silver_x():
    b = spark.read.table("bronze_x")
    # parse, dedup latest-per-key, quarantine handled in a sibling dataset
    ...

@dp.materialized_view
def gold_x():
    return spark.read.table("silver_x").groupBy(...).agg(...)
```
Return the COMPLETE pipeline as a single fenced python block.
