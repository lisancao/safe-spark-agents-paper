---
name: spark-safety
description: General data-correctness best-practices for declarative pipelines — watermarks for bounded state, deterministic dedup on an explicit sequence key, UTC event-time normalization, and quarantine/expectations for unparseable rows.
---

# spark-safety — general correctness guardrails for declarative pipelines

This is the **safety treatment**. It assumes you are already authoring an OSS
Spark Declarative Pipeline (see the `pyspark-sdp` skill for the API). It teaches
GENERAL best-practices for four failure modes that silently corrupt aggregations
without ever raising an error. These are principles to apply with judgement to
whatever you are building — they do not tell you what to compute, and the column
names below (`<entity_key>`, `<event_time>`, …) are placeholders for whatever your
data actually has.

## 1. Watermarks — bound the state of streaming aggregations

For any **streaming** or stateful aggregation, set an **event-time watermark** so
state cannot grow unbounded and late data is handled deterministically:

```python
df.withWatermark("<event_ts>", "2 hours")
```

A watermark only applies to a genuine stream (`@dp.table` over
`spark.readStream`); it is a no-op on a pure batch `@dp.materialized_view`, so do
not bolt one on where there is no streaming state. The principle: never leave a
**stateful streaming aggregation unwatermarked**.

## 2. Deterministic dedup on an explicit sequence key

When upstream data can contain duplicates, deduplicate on the **business key**
using an **explicit, deterministic sequence key** — never `dropDuplicates()` over
all columns (order-dependent, non-deterministic) and never a dedup that depends on
input ordering.

```python
from pyspark.sql import Window, functions as F
# keep the newest record per key; break ties deterministically
w = Window.partitionBy("<entity_key>").orderBy(
        F.col("<event_ts>").desc_nulls_last(), F.col("<tiebreak_seq>").desc())
deduped = (df.withColumn("_rn", F.row_number().over(w))
             .where(F.col("_rn") == 1).drop("_rn"))
```

Principles: partition by the true entity key, order by a real event timestamp or
monotonic sequence (not a column that can tie arbitrarily), and make the
tiebreaker deterministic so the SAME input always yields the SAME survivor.
`dropDuplicates(["<entity_key>"])` keeps an arbitrary row — avoid it when which row
survives matters.

## 3. UTC normalization of event-time

Mixed-timezone / mixed-format event timestamps bucket records into the wrong time
period. Normalize **every** event time to UTC before deriving a date/period, and
handle both epoch and ISO string forms explicitly rather than trusting one parser:

```python
is_epoch = F.col("<event_time>").rlike("^[0-9]+$")
event_ts = F.when(is_epoch, F.timestamp_millis(F.col("<event_time>").cast("long"))) \
            .otherwise(F.to_timestamp("<event_time>"))         # parses ISO-8601
```

Set the session timezone to UTC explicitly rather than relying on the cluster
default (which the spec deliberately does NOT pin, to keep arms symmetric):

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

Do NOT declare an event-time column as a TIMESTAMP directly in a `from_json`
schema if it can arrive as an epoch string: it then misparses (e.g. to a
far-future year) and time-bucketed results silently land in the wrong period. Read
it as a string, branch on the format, normalize.

## 4. Quarantine / expectations for unparseable & invalid rows

Never let malformed input vanish into a null that is later dropped from an
aggregate. Route unparseable / contract-violating rows to a **quarantine** dataset
so the loss is observable, and keep the main path clean. Read external text first,
parse with a schema, then split on parse success (note: keep the original text
column so the quarantine retains it):

```python
@dp.materialized_view(name="quarantine")
def quarantine():
    raw = SparkSession.active().read.text("<input_path>")      # column: value
    parsed = raw.withColumn("j", F.from_json("value", "<schema>"))
    return parsed.where(F.col("j").isNull()).select("value")   # parse failed
```

Apply explicit **expectations** (validity predicates) on the main flow — do not
silently filter the rows that fail them — and send failures to quarantine. General
expectations worth enforcing:

- numeric columns must cast to a non-null number (quote-wrapped/garbage → quarantine)
- a derived date/period must be non-null after UTC normalization
- required keys must be present

Type external columns as **strings first**, validate, then cast — typing them in
the JSON schema turns every bad value into a silent null. The invariant: rows-in
equals (clean rows) + (quarantined rows); aggregates are computed only over rows
that passed every expectation, and nothing is lost without a trace.
