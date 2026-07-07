

## UTC calendar days & timestamp parsing — you CANNOT set session config, so do it in-column

**Hard constraint.** Inside a `@dp.materialized_view` / `@dp.table` function you may **not** call
`spark.conf.set("spark.sql.session.timeZone", ...)` — the shared Connect session rejects it at the
dry-run gate (`CANNOT_MODIFY_CONFIG` / SQLSTATE `46110`). The default session timezone is the
cluster's, **not** UTC. So derive every calendar day from an explicit, session-timezone-**independent**
column expression — never from a session setting, and never from a bare `to_date(to_timestamp(x))`
(that reads `x` in the session tz and buckets on the wrong day).

**Parse robustly — the feed mixes three timestamp shapes; handle all three or rows silently vanish:**
- **offset-aware** (`"...T02:14:00-08:00"`): `F.to_timestamp` resolves the offset to the correct instant.
- **naive** (`"...T16:25:12"`, no offset): these mean **UTC wall-clock**. Tag them as UTC *before*
  parsing so the session tz can't reinterpret them:
  `F.to_timestamp(F.when(c.rlike(r"(Z|[+-]\d{2}:?\d{2})$"), c).otherwise(F.concat(c, F.lit("Z"))))`
- **epoch-millis** (all-digit, ≥13 chars): `F.when(c.rlike(r"^\d{13,}$"), (c.cast("double")/1000).cast("timestamp"))`.
  A missing epoch branch NULLs those rows and they get dropped — a silent undercount.

**Get the UTC calendar day of an instant, independent of the session tz:**
`F.to_date(F.to_utc_timestamp(ts, F.current_timezone()))`
— renders the instant in UTC before taking the date. (A bare `F.to_date(ts)` uses the session tz and
mis-buckets early-UTC-morning instants onto the previous day.)

**Apply the *identical* day-bucketing expression to EVERY joined date column** — the fact/event date
AND the dimension/rate date. Never tz-shift one side and use a bare `to_date` on the other; both sides
must bucket under the same rule or the join lands values on the wrong day.

**Never silently drop unparseable rows** — route them to a rejects/quarantine table and keep
`raw_count == kept_count + rejected_count`.
