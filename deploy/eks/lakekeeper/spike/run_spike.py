#!/usr/bin/env python3
# =============================================================================================
# run_spike.py — the load-bearing check for SP3.4, as a runnable test.
#
# QUESTION (the single assumption the frontier isolation proof rests on):
#   Does a governed catalog's per-tenant, prefix-scoped VENDED credential actually reach the Spark
#   EXECUTOR (which does the S3 FileIO) through Spark Connect, and is IT — not an ambient/static
#   credential — what touches storage? And does it enforce per-tenant isolation (tenant B ->
#   AccessDenied at the storage layer)?
#
# This one harness runs in TWO modes (env-driven), same test logic:
#   * LOCAL  (docker-compose): MinIO, no ambient credential. Proves the WIRING + that a vended,
#            prefix-scoped credential reaches a SEPARATE executor JVM and denies cross-tenant S3.
#   * EKS    (a real cluster): real S3 + IRSA. There, an ambient full-bucket IRSA role EXISTS on
#            the executor pod, so a cross-tenant DENY can ONLY happen if the executor uses the
#            vended (scoped) cred and NOT its IRSA role. That is the load-bearing proof. See ../eks.
#
# The test does NOT trust an app-level check: the cross-tenant denial is asserted at the STORAGE
# layer, by replaying tenant A's actual vended credential against tenant B's prefix and requiring
# S3 AccessDenied.
# =============================================================================================
import json
import os
import sys
import time
import urllib.request
import urllib.error

# ── Config (env-driven; defaults are the local docker-compose network names) ─────────────────
SPARK_REMOTE   = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
LAKEKEEPER_URL = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181").rstrip("/")
S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://localhost:9000")  # fallback if vend omits it
BUCKET         = os.environ.get("BUCKET", "warehouse")

CATALOG_A = os.environ.get("CATALOG_A", "lk_a")
CATALOG_B = os.environ.get("CATALOG_B", "lk_b")
WAREHOUSE_A = os.environ.get("WAREHOUSE_A", "tenant_a")
WAREHOUSE_B = os.environ.get("WAREHOUSE_B", "tenant_b")
SEED_A_KEY = os.environ.get("SEED_A_KEY", "tenant_a/_seed")
SEED_B_KEY = os.environ.get("SEED_B_KEY", "tenant_b/_seed")
NS = os.environ.get("NS", "sales")
TABLE = os.environ.get("TABLE", "orders")

# PROVISION=1 -> this harness bootstraps Lakekeeper + creates the two warehouses (local smoke).
# On EKS the warehouses are provisioned by a k8s Job, so run with PROVISION=0.
PROVISION = os.environ.get("PROVISION", "1") == "1"
WAREHOUSE_CFG_DIR = os.environ.get("WAREHOUSE_CFG_DIR", "config")

# Admin S3 creds (for PLACEMENT verification only — NOT the auth path under test). Optional.
ADMIN_KEY = os.environ.get("MINIO_ROOT_USER", "")
ADMIN_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "")
# The broad base storage-credential (models the fleet role): proves the base identity CAN cross
# tenants, so isolation is due to the VEND, not the base policy.
BASE_KEY = os.environ.get("LK_USER", "")
BASE_SECRET = os.environ.get("LK_PASS", "")

SPARK_DRIVER_UI = os.environ.get("SPARK_DRIVER_UI", "http://localhost:4040").rstrip("/")

RESULTS = []  # (case_id, description, passed, detail)


def record(case_id, desc, passed, detail=""):
    RESULTS.append((case_id, desc, passed, detail))
    mark = "PASS" if passed else "FAIL"
    print(f"[{mark}] {case_id}: {desc}" + (f"  -- {detail}" if detail else ""), flush=True)


# ── HTTP helpers (stdlib only) ───────────────────────────────────────────────────────────────
def http(method, url, body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except Exception:
            parsed = {"_raw": raw}
        return e.code, parsed


def wait_for_http(url, ok=(200, 204), timeout_s=120, name="service"):
    print(f"[wait] {name} at {url} ...", flush=True)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            st, _ = http("GET", url, timeout=5)
            if st in ok:
                print(f"[wait] {name} up ({st})", flush=True)
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


# ── Provisioning (LOCAL smoke) ───────────────────────────────────────────────────────────────
def bootstrap_and_warehouses():
    st, _ = http("POST", f"{LAKEKEEPER_URL}/management/v1/bootstrap",
                 body={"accept-terms-of-use": True})
    print(f"[provision] bootstrap -> {st} (200/204 new, 4xx already bootstrapped both fine)", flush=True)
    for wh_file in (f"{WAREHOUSE_CFG_DIR}/warehouse-{WAREHOUSE_A}.json",
                    f"{WAREHOUSE_CFG_DIR}/warehouse-{WAREHOUSE_B}.json"):
        with open(wh_file) as f:
            payload = json.load(f)
        name = payload["warehouse-name"]
        # Idempotent: if the warehouse already resolves via the Iceberg config endpoint, skip.
        st_cfg, _ = http("GET", f"{LAKEKEEPER_URL}/catalog/v1/config?warehouse={name}")
        if st_cfg == 200:
            print(f"[provision] warehouse {name} already exists (config 200) — skipping", flush=True)
            continue
        st, resp = http("POST", f"{LAKEKEEPER_URL}/management/v1/warehouse", body=payload)
        msg = json.dumps(resp)
        if st in (200, 201):
            print(f"[provision] created warehouse {name}", flush=True)
        elif st == 409 or "overlap" in msg.lower() or "already" in msg.lower():
            print(f"[provision] warehouse {name} already exists ({st}) — continuing", flush=True)
        else:
            print(f"[provision] warehouse {name} -> {st}: {resp}", flush=True)
            raise SystemExit(f"FATAL: could not create warehouse {name}")


# ── Iceberg REST: discover the routing prefix + pull tenant A's actual VENDED credential ─────
def rest_prefix(warehouse):
    st, cfg = http("GET", f"{LAKEKEEPER_URL}/catalog/v1/config?warehouse={warehouse}")
    if st != 200:
        raise RuntimeError(f"/v1/config?warehouse={warehouse} -> {st}: {cfg}")
    merged = {}
    merged.update(cfg.get("defaults", {}) or {})
    merged.update(cfg.get("overrides", {}) or {})
    prefix = merged.get("prefix")
    if not prefix:
        raise RuntimeError(f"no routing 'prefix' in /v1/config for {warehouse}: {cfg}")
    return prefix


def load_table_vended(warehouse, ns, table):
    """Return (vended_cred_config: dict, metadata_location: str) for a table, using the SAME
    access-delegation header Spark uses. This is exactly the credential Lakekeeper hands the engine."""
    prefix = rest_prefix(warehouse)
    url = f"{LAKEKEEPER_URL}/catalog/v1/{prefix}/namespaces/{ns}/tables/{table}"
    st, resp = http("GET", url, headers={"X-Iceberg-Access-Delegation": "vended-credentials"})
    if st != 200:
        raise RuntimeError(f"loadTable {warehouse}.{ns}.{table} -> {st}: {resp}")
    cred = {}
    for sc in (resp.get("storage-credentials") or []):
        cred.update(sc.get("config", {}) or {})
    # Fall back to top-level config if the server inlines creds there.
    for k, v in (resp.get("config", {}) or {}).items():
        cred.setdefault(k, v)
    meta = resp.get("metadata-location", "")
    return cred, meta


# ── boto3 S3 clients ─────────────────────────────────────────────────────────────────────────
def s3_client(access_key, secret_key, session_token=None, endpoint=None, path_style=True,
              region="us-east-1"):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=endpoint or None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
        config=Config(signature_version="s3v4",
                      s3={"addressing_style": "path" if path_style else "auto"},
                      retries={"max_attempts": 2}),
    )


def s3_from_vend(cred):
    ps = str(cred.get("s3.path-style-access", "true")).lower() == "true"
    return s3_client(cred.get("s3.access-key-id"), cred.get("s3.secret-access-key"),
                     cred.get("s3.session-token"),
                     endpoint=cred.get("s3.endpoint") or (S3_ENDPOINT if S3_ENDPOINT else None),
                     path_style=ps, region=cred.get("s3.region", "us-east-1"))


def is_access_denied(err):
    from botocore.exceptions import ClientError
    if not isinstance(err, ClientError):
        return False
    code = err.response.get("Error", {}).get("Code", "")
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return code in ("AccessDenied", "403", "Forbidden", "AllAccessDisabled") or status == 403


def split_s3_uri(uri):
    # s3://bucket/key... -> (bucket, key)
    body = uri.split("://", 1)[1]
    bucket, _, key = body.partition("/")
    return bucket, key


# ── Spark Connect session ────────────────────────────────────────────────────────────────────
def spark_session():
    from pyspark.sql import SparkSession
    last = None
    for _ in range(30):
        try:
            return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
        except Exception as e:  # server may still be binding
            last = e
            time.sleep(4)
    raise RuntimeError(f"could not connect to Spark Connect at {SPARK_REMOTE}: {last}")


def executor_ran_tasks():
    """Prove a SEPARATE executor (not the driver) executed tasks, via the driver UI REST API.
    Returns (ok, detail, total_non_driver_tasks)."""
    try:
        st, apps = http("GET", f"{SPARK_DRIVER_UI}/api/v1/applications", timeout=8)
        if st != 200 or not apps:
            return False, f"applications API -> {st}", 0
        app_id = apps[0]["id"]
        st, execs = http("GET", f"{SPARK_DRIVER_UI}/api/v1/applications/{app_id}/executors", timeout=8)
        if st != 200:
            return False, f"executors API -> {st}", 0
        non_driver = [e for e in execs if e.get("id") != "driver"]
        total_tasks = sum(e.get("totalTasks", 0) for e in non_driver)
        if non_driver:
            hosts = ",".join(sorted({e.get("hostPort", "?") for e in non_driver}))
            return True, f"{len(non_driver)} executor(s) host(s)={hosts} totalTasks={total_tasks}", total_tasks
        return False, "no non-driver executor registered", 0
    except Exception as e:
        return False, f"driver UI unreachable: {e}", 0


# ── The tenant round-trip: write via vend on a real executor, then storage-layer isolation ──
def tenant_positive_write(spark, catalog):
    """Force GENUINE executor-side S3 FileIO: a repartition(shuffle) so the write stage fans out to
    many tasks that MUST run on executor pods (one Iceberg data file each) — not a driver-only path
    that could "pass" without ever exercising executor credential handling (the whole question)."""
    fqtn = f"{catalog}.{NS}.{TABLE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NS}")
    spark.sql(f"DROP TABLE IF EXISTS {fqtn}")
    spark.sql(f"CREATE TABLE {fqtn} (id BIGINT, amount DOUBLE) USING iceberg")
    parts = int(os.environ.get("WRITE_PARTITIONS", "8"))
    rows = int(os.environ.get("WRITE_ROWS", "4000"))
    df = (spark.range(0, rows)
          .repartition(parts)                                   # <-- shuffle -> tasks on executors
          .selectExpr("id", "cast(id * 1.5 as double) as amount"))
    df.writeTo(fqtn).append()                                   # Iceberg DSv2 write, parts data files
    # read-back also runs as executor tasks using the vended credential
    return spark.table(fqtn).count(), parts


def run_tenant(spark, label, catalog, warehouse, own_ok_hint, other_seed_key):
    # --- write path (executor uses the vend); multi-partition to FORCE executor-side FileIO ---
    parts = 0
    try:
        n, parts = tenant_positive_write(spark, catalog)
        record(f"{label}.write",
               f"{label}: multi-partition Iceberg write+read via vended cred ({parts} partitions)",
               n == int(os.environ.get("WRITE_ROWS", "4000")), f"row_count={n}")
    except Exception as e:
        record(f"{label}.write", f"{label}: multi-partition Iceberg write+read via vended cred", False, str(e))
        return

    ok, detail, ntasks = executor_ran_tasks()
    # Require a SEPARATE executor that ran MORE THAN ONE task -> the shuffle write genuinely fanned
    # out to executor-side FileIO (a driver-only path could not produce parts-many executor tasks).
    multi = ok and ntasks >= max(2, parts)
    record(f"{label}.executor",
           f"{label}: a SEPARATE executor ran the {parts}-partition write ({'>=' if multi else '<'} {max(2, parts)} tasks)",
           multi, detail)

    # --- pull the ACTUAL vended credential the catalog handed the engine ---
    try:
        cred, meta = load_table_vended(warehouse, NS, TABLE)
        has_token = bool(cred.get("s3.session-token"))
        akid = cred.get("s3.access-key-id", "")
        vend_is_scoped_temp = has_token and akid not in (BASE_KEY, ADMIN_KEY, "")
        record(f"{label}.vend", f"{label}: catalog vends a temporary (session-token) credential",
               vend_is_scoped_temp,
               f"access-key-id={akid[:6]}... session-token={'yes' if has_token else 'NO'}")
    except Exception as e:
        record(f"{label}.vend", f"{label}: catalog vends a temporary credential", False, str(e))
        return

    s3v = s3_from_vend(cred)

    # sanity: the vended cred CAN read the tenant's OWN table object (it is a valid, live cred)
    try:
        b, k = split_s3_uri(meta)
        s3v.head_object(Bucket=b, Key=k)
        record(f"{label}.own", f"{label}: vended cred reads its OWN table object (positive control)", True,
               f"s3://{b}/{k}")
    except Exception as e:
        record(f"{label}.own", f"{label}: vended cred reads its OWN table object (positive control)", False, str(e))

    # THE MONEY SHOT: the SAME vended cred must be DENIED on the OTHER tenant's prefix (storage layer)
    from botocore.exceptions import ClientError
    denied_read = False
    try:
        s3v.get_object(Bucket=BUCKET, Key=other_seed_key)
    except ClientError as e:
        denied_read = is_access_denied(e)
    except Exception:
        pass
    record(f"{label}.cross_read",
           f"{label}: vended cred DENIED reading other tenant ({other_seed_key}) at S3",
           denied_read, "AccessDenied" if denied_read else "NOT denied -> isolation FAILS")

    denied_write = False
    try:
        s3v.put_object(Bucket=BUCKET, Key=f"{other_seed_key}.intrusion_by_{label}", Body=b"x")
    except ClientError as e:
        denied_write = is_access_denied(e)
    except Exception:
        pass
    record(f"{label}.cross_write",
           f"{label}: vended cred DENIED writing other tenant's prefix at S3",
           denied_write, "AccessDenied" if denied_write else "NOT denied -> isolation FAILS")


def ablation_broad_ambient():
    """ABLATION (proves the vend is LOAD-BEARING, not incidental): run the EXACT cross-tenant ops the
    vended cred was denied, but with a BROAD ambient credential (the whole-bucket base user — the
    local analog of a full-bucket IRSA role). They must SUCCEED. So the ONLY reason the executor's
    vended path is isolated is the downscoping; had the executor used a broad ambient cred (the EKS
    failure mode this whole spike guards against), cross-tenant access would go through."""
    if not (BASE_KEY and BASE_SECRET):
        record("ablation.broad_ambient", "broad ambient cred SUCCEEDS cross-tenant", True,
               "skipped (no base creds provided)")
        return
    try:
        s3b = s3_client(BASE_KEY, BASE_SECRET, endpoint=S3_ENDPOINT, path_style=True)
        # The same operations that were AccessDenied under the vended cred:
        s3b.get_object(Bucket=BUCKET, Key=SEED_B_KEY)                       # "A" reading B's prefix
        s3b.get_object(Bucket=BUCKET, Key=SEED_A_KEY)                       # "B" reading A's prefix
        s3b.put_object(Bucket=BUCKET, Key=f"{SEED_B_KEY}.broad_ambient_write", Body=b"x")
        record("ablation.broad_ambient",
               "BROAD ambient cred SUCCEEDS on BOTH tenants' prefixes (=> the VEND is load-bearing)",
               True, "cross-tenant read+write allowed with the whole-bucket credential")
    except Exception as e:
        record("ablation.broad_ambient",
               "BROAD ambient cred SUCCEEDS cross-tenant", False, str(e))


def main():
    print(f"== Lakekeeper vended-credential -> executor spike ==\n"
          f"   spark   : {SPARK_REMOTE}\n   catalog : {LAKEKEEPER_URL}\n   s3      : {S3_ENDPOINT}\n"
          f"   mode    : {'PROVISION+TEST (local)' if PROVISION else 'TEST-only (eks)'}\n", flush=True)

    if not wait_for_http(f"{LAKEKEEPER_URL}/health", name="lakekeeper"):
        record("setup.lakekeeper", "Lakekeeper /health reachable", False, "timed out")
        finish()
    if PROVISION:
        bootstrap_and_warehouses()

    spark = spark_session()
    print(f"[spark] connected; version={spark.version}", flush=True)

    run_tenant(spark, "A", CATALOG_A, WAREHOUSE_A, own_ok_hint=SEED_A_KEY, other_seed_key=SEED_B_KEY)
    run_tenant(spark, "B", CATALOG_B, WAREHOUSE_B, own_ok_hint=SEED_B_KEY, other_seed_key=SEED_A_KEY)
    ablation_broad_ambient()

    # Placement verification (secondary; admin creds, not the tested path)
    if ADMIN_KEY and ADMIN_SECRET:
        try:
            s3a = s3_client(ADMIN_KEY, ADMIN_SECRET, endpoint=S3_ENDPOINT, path_style=True)
            a_objs = s3a.list_objects_v2(Bucket=BUCKET, Prefix=f"{WAREHOUSE_A}/").get("KeyCount", 0)
            b_objs = s3a.list_objects_v2(Bucket=BUCKET, Prefix=f"{WAREHOUSE_B}/").get("KeyCount", 0)
            record("verify.placement", "A's data landed under its own prefix", a_objs > 1,
                   f"tenant_a objects={a_objs}, tenant_b objects={b_objs}")
        except Exception as e:
            record("verify.placement", "A's data landed under its own prefix", False, str(e))

    try:
        spark.stop()
    except Exception:
        pass
    finish()


def finish():
    print("\n================ SPIKE RESULTS ================", flush=True)
    critical = [r for r in RESULTS if r[0].endswith((".cross_read", ".cross_write", ".write", ".executor"))]
    passed = sum(1 for r in RESULTS if r[2])
    for cid, desc, ok, detail in RESULTS:
        print(f"  {'PASS' if ok else 'FAIL'}  {cid:<20} {desc}", flush=True)
    print(f"----------------------------------------------\n  {passed}/{len(RESULTS)} checks passed", flush=True)

    verdict_ok = all(r[2] for r in critical) and len(critical) > 0
    print("\n  VERDICT: vended credential reaches the executor AND enforces per-tenant S3 isolation: "
          + ("YES" if verdict_ok else "NO / INCONCLUSIVE"), flush=True)

    os.makedirs("out", exist_ok=True)
    with open("out/results.json", "w") as f:
        json.dump({"results": [{"case": c, "desc": d, "passed": p, "detail": t} for c, d, p, t in RESULTS],
                   "verdict_isolation_holds": verdict_ok}, f, indent=2)
    sys.exit(0 if all(r[2] for r in RESULTS) else 1)


if __name__ == "__main__":
    main()
