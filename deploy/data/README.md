# `deploy/data/` — Real + Messy Data Layer (Iceberg on S3 via HMS)

The data layer for the reference architecture: a deterministic **food-delivery**
data generator with a **chaos engine** (the "real + messy" requirement), plus
two ingest paths that land the data in **Iceberg bronze tables on S3** through a
**Hive Metastore (HMS)** catalog:

- **Batch:** generate parquet → `spark-submit load_to_iceberg.py` → `<catalog>.bronze.*`
- **Streaming:** generate parquet → `python -m generator stream` → Kafka → `spark-submit stream_to_iceberg.py` → `<catalog>.bronze.orders_stream`

Ported and adapted from `lakehouse-stack/scripts/testdata/` (generator + chaos +
producer) and `lakehouse-stack/data/load_to_iceberg.py` (loader). The original
catalog was hardcoded to `iceberg.bronze`; here everything (catalog name, HMS
URI, S3 bucket, Kafka bootstrap, topic) is parameterized.

## The domain

Each order walks an order → kitchen → driver → delivery lifecycle, emitting a
stream of typed events:

```
order_created → kitchen_started → kitchen_finished → order_ready
              → driver_arrived → driver_picked_up → driver_ping* → delivered
```

with `order_cancelled` as an early-terminal branch (when `--cancel-rate > 0`).
Demand follows realistic hour-of-day and day-of-week curves (Poisson arrivals);
brands have momentum (growing/declining) that shifts the mix over time.

Four **dimension** tables accompany the events: `items`, `categories`, `brands`,
`locations` (San Francisco, Silicon Valley, Seattle, Austin).

### Event schema (bronze `orders`)

| column            | type      | notes                                            |
|-------------------|-----------|--------------------------------------------------|
| `event_id`        | string    | uuid                                             |
| `event_type`      | string    | one of the lifecycle types above                 |
| `ts`              | string    | ISO-8601; **kept raw** (chaos may corrupt it)    |
| `ts_seconds`      | bigint    | epoch seconds                                    |
| `location_id`     | int       | FK → `dim_locations` (nullable; chaos nulls it)  |
| `order_id`        | string    | 6-char id; groups an order's events              |
| `sequence`        | int       | per-order event ordinal                          |
| `body`            | string    | JSON payload, type-specific; **kept raw**        |
| `event_timestamp` | timestamp | parsed from `ts` by the loader (added column)    |

The streaming table `orders_stream` adds `raw_value` (the original Kafka JSON)
and `ingest_ts`.

## The chaos engine (the "messy" requirement)

`generator/chaos.py` injects four classes of data-quality defect at configurable
rates. Knobs live in [`chaos.yaml`](chaos.yaml):

| knob                  | default | injects                                              |
|-----------------------|---------|------------------------------------------------------|
| `null_rate`           | 0.05    | a nulled field (`location_id`/`order_id`/body key)   |
| `late_event_rate`     | 0.03    | delayed / out-of-order (shuffled) events             |
| `duplicate_rate`      | 0.02    | duplicate events (sometimes w/ bumped timestamp)     |
| `malformed_json_rate` | 0.01    | corrupted `body` JSON (truncated / missing brace…)   |
| `seed`                | 42      | RNG seed for the **streaming** ChaosMonkey           |

Bronze is **permissive on purpose**: malformed records are retained (the loader
parses with `to_timestamp`/`from_json`, which yield nulls rather than throwing),
so silver owns cleansing/dedup/quarantine. Disable with `--no-chaos` or
`enabled: false`.

**Event-time vs. arrival-order.** `late_event_rate` is realized as genuinely
out-of-order *rows* in the written/streamed data, **not** as a perturbed
timestamp: the batch exporter sorts each batch by event time to set a clean
arrival baseline, then chaos reorders rows, and the result is written **without
re-sorting** (so the disorder survives). The `ts`/`ts_seconds` event-time columns
stay intact — downstream watermarking sees late arrivals against true event time.

**Determinism** is preserved end-to-end and on **both** paths:
- *Batch:* `generate_all_events` seeds `numpy` and `random` from `config.seed`
  (default 42); `event_id` is drawn from that seeded stream (not `uuid.uuid4`).
- *Streaming:* the replay never calls `generate_all_events`, so the `ChaosMonkey`
  carries its **own** `random.Random(chaos.seed)` (set via `--seed`) — same
  (parquet, args, seed) ⇒ identical Kafka output.

Same seed + args ⇒ byte-identical dataset, which is why the ~7.5 GB output is
**not committed** (see [`.gitignore`](.gitignore)); it is regenerated, not stored.

## Medallion namespace convention

The pipelines expect a medallion layout under the catalog. The loaders provision
all three namespaces and write **bronze** only:

```
<catalog>.bronze.*   raw, messy, append-only   ← this layer writes here
  ├─ orders                 (batch events)
  ├─ orders_stream          (streaming events)
  ├─ dim_categories / dim_brands / dim_items / dim_locations
<catalog>.silver.*   cleansed / deduplicated / quarantine-split   (downstream)
<catalog>.gold.*     business aggregates                          (downstream)
```

Default catalog name is `lakehouse` (override with `--catalog` / `ICEBERG_CATALOG`).

## Layout

```
deploy/data/
├── README.md                 ← you are here
├── chaos.yaml                ← chaos knobs
├── requirements.txt          ← generator/producer deps (loader uses cluster PySpark)
├── generator/                ← ported generator + chaos engine (a Python package)
│   ├── __main__.py             CLI: generate / stream / stats / clean
│   ├── config.py               GeneratorConfig, ChaosConfig, YAML + env loaders
│   ├── dimensions.py           items / categories / brands / locations  (verbatim port)
│   ├── events.py               order-lifecycle event generator           (verbatim port)
│   ├── chaos.py                null/malformed/dupe/late injection         (verbatim port)
│   ├── exporter.py             batch parquet writer
│   └── producer.py             Kafka streaming producer (parquet replay)
└── load/                      ← Iceberg ingest (run under spark-submit)
    ├── spark_catalog.py        SparkSession ⇄ Iceberg/HMS/S3 wiring (parameterized)
    ├── load_to_iceberg.py      BATCH: parquet → bronze (USING iceberg DDL)
    ├── stream_to_iceberg.py    STREAM: Kafka → bronze (Structured Streaming)
    └── spark-defaults.iceberg.template.conf   alt. catalog wiring via conf
```

## Parameters (CLI flag > env var > default)

| concern        | flag                  | env                 | default                          |
|----------------|-----------------------|---------------------|----------------------------------|
| catalog name   | `--catalog`           | `ICEBERG_CATALOG`   | `lakehouse`                      |
| HMS thrift URI | `--hms-uri`           | `HMS_URI`           | `thrift://hive-metastore:9083`   |
| S3 bucket      | `--bucket`            | `S3_BUCKET`         | *(required)*                     |
| warehouse URI  | `--warehouse`         | `ICEBERG_WAREHOUSE` | `s3://<bucket>/warehouse`        |
| Kafka bootstrap| `--kafka`             | `KAFKA_BOOTSTRAP`   | `kafka:9092`                     |
| Kafka topic    | `--topic`             | `KAFKA_TOPIC`       | `orders`                         |
| stream ckpt    | `--checkpoint`        | `STREAM_CHECKPOINT` | *(required for streaming)*       |
| data dir       | `--data-dir`/`--output`| `DATA_OUTPUT_DIR`  | `/data` (load) / `data` (gen)    |

S3 auth is **IRSA** (IAM Roles for Service Accounts): Iceberg `S3FileIO` uses the
AWS SDK v2 default credential chain, which reads the projected web-identity token
automatically — no keys are configured. `s3a` (for `s3://` *source* reads) is
pointed at `WebIdentityTokenFileCredentialsProvider` for parity. See
[`load/spark_catalog.py`](load/spark_catalog.py).

## How to run

### 0. Install generator deps

```bash
cd deploy/data
pip install -r requirements.txt
```

### 1. Generate the dataset (deterministic)

```bash
# Small/dev (7 days, ~minutes); chaos from chaos.yaml
python -m generator generate --days 7 --chaos-config chaos.yaml

# Full reference set (90 days ⇒ ~7.5 GB parquet — NOT committed)
python -m generator generate --days 90 --chaos-config chaos.yaml

# Clean dataset (no defects)
python -m generator generate --days 7 --no-chaos

python -m generator stats     # inspect counts/date-range/event-type mix
python -m generator clean     # remove generated data/
```

Output lands in `data/dimensions/*.parquet` and `data/events/orders_<days>d.parquet`.

### 2a. Batch load → Iceberg bronze

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
  load/load_to_iceberg.py \
    --bucket  my-lakehouse-bucket \
    --catalog lakehouse \
    --data-dir ./data
```

Creates `lakehouse.bronze.{dim_categories,dim_brands,dim_items,dim_locations,orders}`
as Iceberg tables (`USING iceberg`, via the v2 `writeTo(...).using("iceberg")`
writer) in the HMS catalog, data on `s3://my-lakehouse-bucket/warehouse`.

### 2b. Stream load → Iceberg bronze

Terminal A — replay the messy events into Kafka (speed multiplier: 1 real min =
`speed` simulated hours):

```bash
python -m generator stream --days 7 --kafka kafka:9092 --topic orders --speed 60 \
  --chaos-config chaos.yaml --seed 42
# streaming chaos rates are as configurable as batch: --chaos-config (YAML),
# --chaos-rate (single knob), --no-chaos, and --seed (deterministic replay).
```

Terminal B — consume Kafka into Iceberg bronze:

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  load/stream_to_iceberg.py \
    --bucket my-lakehouse-bucket --catalog lakehouse \
    --kafka  kafka:9092 --topic orders \
    --checkpoint s3://my-lakehouse-bucket/checkpoints/orders_stream
# add --once for a single availableNow micro-batch instead of running forever
```

## Spark version notes

Kept Spark-version-agnostic where possible:

- The loaders use only stable APIs (`writeTo(...).using("iceberg")`, DataFrame
  reads, Structured Streaming `format("iceberg")`, `from_json`/`to_timestamp`)
  and read **all** catalog/version specifics from flags/env — no version is
  baked in.
- Validated target: **released Spark 4.1 + Iceberg 1.11.0**. The `--packages`
  examples pin the `3.5_2.12` runtime coordinate; **swap the Spark suffix to
  match your cluster** (Iceberg 1.11.0 ships `iceberg-spark-runtime-{3.5,4.0,4.1}`
  runtimes), or set `ICEBERG_SPARK_RUNTIME` and let `spark_catalog.py` add it to
  `spark.jars.packages`.
- Nothing here requires the **Tier-B Spark 5.0-snapshot**. If a future pipeline
  needs a 5.0-only feature (e.g. newer SQL/streaming surface), that would be the
  one place to note a 5.0-snapshot dependency — the generator and these loaders
  do not.

## Gates / validation

This layer is validated at the **syntax** level and does not start Spark/S3/Kafka:

```bash
python -m py_compile deploy/data/generator/*.py deploy/data/load/*.py   # all clean
```

Running against a live cluster (HMS + S3 + Kafka) is the **post-deploy** step,
documented above — intentionally not faked here.
