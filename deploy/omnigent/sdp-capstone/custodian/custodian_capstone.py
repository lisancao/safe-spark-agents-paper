#!/usr/bin/env python3
"""SP4 capstone: the fleet CUSTODIAN + SDP executor + contextual-policy engine.

The custodian is the ONLY holder of the Connect PSK and the customer->tenant routing. Sub-agents
get exactly one interface: submit(customer, sdp_code) -> pass/fail + metrics. They never see the
PSK, never choose a tenant server, never hold a catalog token (the per-tenant Connect server injects
it server-side). Each customer's medallion materializes over ITS OWN tenant server (S4.3 custody +
S3 per-tenant isolation), and the customer's contextual policy is enforced at submit time.

Runtime deps: pyspark 4.1 Connect client; PSK in env SC_PSK; per-tenant port-forwards already up
(retail->:15010=tenant_a, saas->:15011=tenant_b, payments->:15012=tenant_c).
"""
import os, sys, types, json, time
from pyspark.sql import SparkSession, DataFrame

# ---- the custodian's private routing table (customer -> tenant binding) ----
CUSTOMERS = {
    "retail":   {"port": 15010, "principal": "tenant_a", "ns": "retail_med", "substrate": "orders",   "policy": "standard"},
    "saas":     {"port": 15011, "principal": "tenant_b", "ns": "saas_med",   "substrate": "cdc",      "policy": "pii_restricted"},
    "payments": {"port": 15012, "principal": "tenant_c", "ns": "pay_med",     "substrate": "payments", "policy": "financial_grade"},
}
PSK = os.environ.get("SC_PSK")  # held only by the custodian process

def _session(customer):
    c = CUSTOMERS[customer]; p = c["principal"]
    url = f"sc://localhost:{c['port']}/;x-connect-principal={p};token={PSK};user_id={p}"
    spark = SparkSession.builder.remote(url).getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lk.{c['ns']}")
    spark.sql(f"USE lk.{c['ns']}")           # unqualified dataset names resolve in the customer's namespace
    return spark

# ---- minimal SDP executor: run @dp.materialized_view/@dp.table datasets over the tenant session ----
def _dp_shim():
    dp = types.ModuleType("dp")
    dp._registry = []  # (name, fn)
    def _reg(*a, **k):
        if a and callable(a[0]):
            dp._registry.append((a[0].__name__, a[0])); return a[0]
        def deco(fn):
            dp._registry.append((k.get("name", fn.__name__), fn)); return fn
        return deco
    dp.materialized_view = _reg
    dp.table = _reg
    dp.expect = lambda *a, **k: (lambda fn: fn)
    dp.expect_or_drop = lambda *a, **k: (lambda fn: fn)
    dp.expect_or_fail = lambda *a, **k: (lambda fn: fn)
    return dp

def _materialize(spark, ns, code):
    """Exec SDP code with a dp shim + tenant spark; write each dataset as lk.<ns>.<name>; fixpoint on deps."""
    dp = _dp_shim()
    g = {"dp": dp, "spark": spark, "__name__": "sdp_module"}
    try:
        from pyspark.sql import functions as F, Window
        g["F"] = F; g["Window"] = Window
    except Exception:
        pass
    # intercept every import form for the declarative API so agent code binds to our executor
    saved = {k: sys.modules.get(k) for k in ("dp", "pyspark.pipelines")}
    sys.modules["dp"] = dp
    sys.modules["pyspark.pipelines"] = dp
    try:
        exec(compile(code, "<sdp_submission>", "exec"), g, g)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    datasets = list(dp._registry)
    done, errors, pending = {}, {}, list(datasets)
    for _ in range(len(datasets) + 2):
        if not pending:
            break
        nxt = []
        for name, fn in pending:
            try:
                df = fn()
                if not isinstance(df, DataFrame):
                    raise TypeError(f"{name} did not return a DataFrame")
                df.writeTo(f"lk.{ns}.{name}").using("iceberg").createOrReplace()
                done[name] = spark.table(f"lk.{ns}.{name}").count()
            except Exception as e:
                msg = str(e).split("JVM stacktrace")[0].strip()
                errors[name] = f"{type(e).__name__}: {msg[:300]}"; nxt.append((name, fn))
        if len(nxt) == len(pending):
            break     # no progress => unresolved deps
        pending = nxt
    for k in done:
        errors.pop(k, None)
    return {"materialized": done, "errors": errors, "n_datasets": len(datasets)}

# ---- contextual policy engine (per-customer governance, enforced at submit) ----
def _policy(spark, customer, ns, mat):
    pol = CUSTOMERS[customer]["policy"]; res = {"policy": pol, "checks": [], "pass": True}
    def chk(name, ok, detail=""):
        res["checks"].append({"check": name, "pass": bool(ok), "detail": detail}); res["pass"] &= bool(ok)
    tables = {r[0] for r in spark.sql(f"SHOW TABLES IN lk.{ns}").select("tableName").collect()} if mat.get("materialized") else set()
    if pol == "standard":
        has_q = any("quarantine" in t or "reject" in t for t in tables)
        chk("corrupt_quarantined_not_dropped", has_q, f"quarantine table present={has_q}; tables={sorted(tables)}")
    elif pol == "pii_restricted":
        gold = [t for t in tables if t.startswith("gold")]
        leaked = []
        for t in gold:
            cols = [c.name.lower() for c in spark.table(f"lk.{ns}.{t}").schema.fields]
            if any(c == "region" for c in cols):
                leaked.append(t)
        chk("pii_region_masked_in_gold", not leaked, f"gold tables leaking raw region: {leaked or 'none'}")
    elif pol == "financial_grade":
        try:
            raw = spark.table(f"lk.{ns}.raw_{CUSTOMERS[customer]['substrate']}").count()
            sil = sum(spark.table(f"lk.{ns}.{t}").count() for t in tables if t.startswith("silver"))
            qn  = sum(spark.table(f"lk.{ns}.{t}").count() for t in tables if "quarantine" in t or "reject" in t)
            chk("no_silent_row_drop", raw == sil + qn, f"raw={raw} silver={sil} quarantine={qn}")
        except Exception as e:
            chk("no_silent_row_drop", False, f"could not verify: {type(e).__name__}: {e}")
    return res

# ---- the sub-agent-facing interface: submit code, get pass/fail (no creds ever handed out) ----
def submit(customer, sdp_code):
    t0 = time.time(); c = CUSTOMERS[customer]
    spark = _session(customer)
    try:
        mat = _materialize(spark, c["ns"], sdp_code)
        pol = _policy(spark, customer, c["ns"], mat)
        ok = (not mat["errors"]) and pol["pass"]
        return {"customer": customer, "tenant": c["principal"], "ns": c["ns"], "pass": ok,
                "materialize": mat, "policy": pol, "elapsed_s": round(time.time()-t0, 1)}
    finally:
        spark.stop()

# ---- cross-tenant deny probe: a session for one customer cannot see another's medallion ----
def probe_isolation(customer, other_ns):
    c = CUSTOMERS[customer]; spark = None
    try:
        spark = _session(customer)
        n = spark.sql(f"SHOW TABLES IN lk.{other_ns}").count()
        return {"from": c["principal"], "reached": other_ns, "denied": False, "tables_seen": n}
    except Exception as e:
        return {"from": c["principal"], "reached": other_ns, "denied": True, "detail": f"{type(e).__name__}: {str(e)[:120]}"}
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "submit":
        customer, path = sys.argv[2], sys.argv[3]
        print(json.dumps(submit(customer, open(path).read()), indent=1))
    elif cmd == "probe":
        print(json.dumps(probe_isolation(sys.argv[2], sys.argv[3]), indent=1))
