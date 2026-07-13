# What kinds of hallucinations show up: imperative (A) vs SDP (B)

*Section 1 supplementary analysis (not in the powered headline; a new dimension on the existing raw
data). Reproduce: `python3 study/analysis/hallucination_taxonomy.py` over
`study/raw/raw_20260628/all_results.jsonl` (the N=3 sweeps: imperative A/A2 = 158 runs, SDP B/B1 = 145
runs). A "hallucination" here means the agent invented something that does not exist, or wrote code
for the wrong paradigm; pure logic/data defects (the silent-defect D1-D9 axis, ambiguous joins, bad
casts) are tracked separately and excluded here.*

## Headline
The two arms do not just fail at different rates, they hallucinate **different things**, caught in
**different places**:

| | imperative (A/A2) | SDP (B/B1) |
|---|---|---|
| dominant hallucination | **inventing I/O paths** (where data lives) | **paradigm confusion** (imperative habits inside a declarative pipeline) |
| where caught | almost only at **runtime** (96% of error-iterations) | **39% at the cheap dry-run gate**, before any data moves |
| cost signature | runtime fail-loops (49 runs hit the iteration cap) | structural rejection, agent fixes and moves on (8 runs hit the cap) |

## Taxonomy (runs affected, by arm)
| hallucination category | imperative | SDP |
|---|---:|---:|
| wrong-paradigm: imperative session control (`spark.conf.set`) in a declarative pipeline | 0 | **76** |
| invented / undeclared table or view | 1 | **40** |
| wrong-paradigm: eager action (`.collect()`/`.show()`) inside a declarative query function | 0 | **27** |
| invented column name | 3 | **21** |
| **invented I/O path** (nonexistent input/output location) | **51** | 0 |
| invented / unsupported API (`ATTRIBUTE_NOT_SUPPORTED`) | **4** | 0 |

## Imperative (A): it invents where the data lives
The dominant imperative hallucination is `OUTPUT_PATH_NOT_FOUND`: the agent hard-codes or guesses input
and output paths that do not exist. It is un-gateable (nothing structural can know a path is wrong
until you touch storage), so it surfaces only at **execute** (555 error-iterations, all at runtime),
and the agent loops, re-guessing paths. This is a large part of why the imperative arm burns compute.
Examples (from final authored programs):
```
DEFAULT_INPUT = "/tmp/.../_data/gen_messy_orders_seed1337.ndjson"   # a specific invented path
raw = spark.read.parquet(input_path)                               # input_path never resolves
```
A smaller, sharper class is `ATTRIBUTE_NOT_SUPPORTED`: calling a method the API does not have.

## SDP (B): it confuses the paradigm, and the gate catches it
The SDP agent's hallucinations are mostly the agent forgetting it is in a declarative framework and
writing imperative code. The two biggest:
- **Imperative session control** (`SESSION_MUTATION_IN_DECLARATIVE_PIPELINE`): the agent writes
  `spark.conf.set(...)` inside a pipeline. SDP forbids it (the framework owns the session). 76 runs did
  this; **80 of the 96 occurrences are caught at the dry-run gate**, and the agent then removes it, so
  it survives in essentially **0 final SDP programs**. The identical line is legitimate in the
  imperative arm (63 final programs keep it, no error), which is the point: the *same* keystroke is a
  caught hallucination in one paradigm and correct in the other.
  ```
  SparkSession.active().conf.set("spark.sql.session.timeZone", "UTC")   # gate-rejected in SDP
  ```
- **Eager actions in a query function** (`ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`): `.collect()`,
  `.count()`, `.show()` inside a declarative dataset function (27 runs).

The SDP agent also invents **undeclared tables/views** (`TABLE_OR_VIEW_NOT_FOUND`, 40 runs) and
**columns** (`UNRESOLVED_COLUMN.WITH_SUGGESTION`, 21 runs), the declarative analog of the imperative
arm's invented paths, but referenced by catalog name rather than filesystem path.
```
return SparkSession.active().read.table("orders_parsed")            # never declared as an upstream
.read.table("spark_catalog.default.clean_orders")                  # invented fully-qualified name
```

## The DLT null (an honest, informative zero)
Neither shipped arm hallucinated Databricks DLT (`import dlt`, `@dlt.table`, `dbutils`): **0 in both**.
Imperative has no framework to confuse; the SDP arm ships with the governed `pyspark-sdp` skill, which
is what keeps it on the OSS `pyspark.pipelines` API. The counterfactual (an SDP agent *without* the
skill falling back to DLT) is a separate ablation and is not in this dataset; do not read this zero as
evidence about the no-skill condition.

## Why this matters for Section 1
Section 1's headline is that SDP's dry-run catches 79 structural **defects** vs 0. This analysis adds a
second, orthogonal thing the same gate does: it catches a whole **class of agent hallucination**
(paradigm-confusion) cheaply, before compute. The imperative arm's dominant hallucination (invented
I/O paths) is fundamentally un-gateable and only shows up at runtime, which is exactly where its
compute blowup comes from. The arms are not more-vs-less hallucinating; they hallucinate different
things, and the declarative gate moves a big share of them from expensive-runtime to cheap-structural.

## Caveats
- `error_class` is the harness's own structured label per iteration; the mapping to "hallucination"
  categories is ours (see the script). Logic/data defects are excluded.
- `final_program` shows what *persisted*; gate-caught hallucinations are fixed by the final iteration
  (hence ~0 in final SDP programs), so counts of the *event* come from `per_iteration`, not the final code.
- These are the N=3 exploratory sweeps, not the powered run; treat magnitudes as indicative.
