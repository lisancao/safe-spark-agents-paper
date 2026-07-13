#!/usr/bin/env python3
"""Seed messy raw data into a customer's tenant namespace (bronze source for the medallion).
Runs THROUGH the custodian session (tenant-isolated write to the customer's own prefix)."""
import sys, json
from custodian_capstone import CUSTOMERS, _session

def _write(spark, ns, name, rows, schema):
    df = spark.createDataFrame(rows, schema)
    df.writeTo(f"lk.{ns}.{name}").using("iceberg").createOrReplace()
    return spark.table(f"lk.{ns}.{name}").count()

def seed(customer):
    c = CUSTOMERS[customer]; ns = c["ns"]; sub = c["substrate"]
    spark = _session(customer)
    out = {}
    try:
        if sub == "orders":
            rows = [
                ("o1","2026-07-01T10:00:00","12.50","USD","books"),
                ("o1","2026-07-01T11:00:00","12.50","USD","books"),     # dup: later ts wins
                ("o2","2026-07-01T12:00:00","$30.00","USD","toys"),
                ("o3","2026-07-02T09:00:00","1,250.00","USD","electronics"),
                ("o4","2026-07-02T23:30:00","5.00","USD","books"),
                ("o5","2099-01-01T00:00:00","9.99","USD","books"),      # future ts: dropped from silver
                ("o6","2026-07-03T08:00:00","abc","USD","toys"),         # bad amount: quarantine
                (None,"2026-07-03T08:00:00","7.00","USD","toys"),        # null id: quarantine
                ("o7","2026-07-03T15:00:00","20.00","USD","garden"),
                ("o8","2026-07-03T16:00:00","40.00","USD","toys"),
                ("o9","2026-07-04T01:00:00","15.00","USD","books"),
                ("o9","2026-07-04T02:00:00","15.00","USD","books"),      # dup
            ]
            out["raw_orders"] = _write(spark, ns, "raw_orders", rows,
                "order_id string, event_ts string, amount string, currency string, category string")
        elif sub == "cdc":
            rows = [  # customer_id, op, tier, region, seq (out of order)
                ("c1","I","free","us",   1),
                ("c1","U","pro","us",    3),
                ("c1","U","pro","eu",    2),      # out-of-order: seq 2 arrives after 3
                ("c2","I","free","apac", 1),
                ("c2","D",None,None,     2),        # delete: c2 removed from current
                ("c3","I","pro","eu",    1),
                ("c3","U","enterprise","eu", 2),
                ("c1","I","free","us",   1),        # duplicate insert (idempotency)
            ]
            out["raw_cdc"] = _write(spark, ns, "raw_cdc", rows,
                "customer_id string, op string, tier string, region string, seq int")
        elif sub == "payments":
            rows = [  # payment_id, event_ts, amount, currency
                ("p1","2026-07-01T10:00:00","100.00","USD"),
                ("p2","2026-07-01T12:00:00","90.00","EUR"),
                ("p3","2026-07-02T09:00:00","5000.00","JPY"),
                ("p4","2026-07-02T14:00:00","50.00","GBP"),
                ("p5","2026-07-03T08:00:00","75.00","XYZ"),   # unknown currency: quarantine, not dropped
                ("p6","2026-07-03T16:00:00","20.00","USD"),
            ]
            out["raw_payments"] = _write(spark, ns, "raw_payments", rows,
                "payment_id string, event_ts string, amount string, currency string")
            fx = [  # currency, rate_date, usd_rate
                ("USD","2026-07-01",1.0),("EUR","2026-07-01",1.08),("JPY","2026-07-02",0.0064),
                ("GBP","2026-07-02",1.27),("USD","2026-07-03",1.0),
            ]
            out["raw_fx"] = _write(spark, ns, "raw_fx", fx, "currency string, rate_date string, usd_rate double")
        return {"customer": customer, "ns": ns, "seeded": out}
    finally:
        spark.stop()

if __name__ == "__main__":
    print(json.dumps(seed(sys.argv[1]), indent=1))
