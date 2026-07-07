#!/usr/bin/env python3
"""Compare D7 timezone/day-bucket ships (arm B): baseline (primary powered run) vs tzfix (UTC-skill run).
Usage: compare_d7.py [tzfix_results.jsonl] [baseline_results.jsonl]
Defaults: <study>/results.tzfix.jsonl  and  <study>/results.powered.AB.n12.final.jsonl (or results.powered.final.jsonl)."""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.normpath(os.path.join(HERE, "..", ".."))
TASKS = ["p8_currency_normalize", "p14_fx_settlement", "new_stream_stream_join"]

def load(p): return [json.loads(l) for l in open(p) if l.strip()] if p and os.path.exists(p) else []
def tid(r): return r.get("task") or r.get("task_id")
def d7(rows, t): return sum(1 for r in rows if tid(r) == t and r.get("arm") == "B"
                            and r.get("per_defect_detection", {}).get("D7") == "never")

tzfix_p = sys.argv[1] if len(sys.argv) > 1 else os.path.join(STUDY, "results.tzfix.jsonl")
base_p = sys.argv[2] if len(sys.argv) > 2 else next(
    (os.path.join(STUDY, f) for f in ("results.powered.AB.n12.final.jsonl", "results.powered.final.jsonl")
     if os.path.exists(os.path.join(STUDY, f))), None)
base, tz = load(base_p), load(tzfix_p)

print(f"=== D7 ships (arm B): baseline vs tzfix ===  spark={__import__('pyspark').__version__}")
print(f"  baseline: {base_p or '(none found)'}")
print(f"  tzfix:    {tzfix_p}")
tb = tt = 0
for t in TASKS:
    b, x = d7(base, t), d7(tz, t)
    tb += b; tt += x
    print(f"    {t:26} baseline B-D7={b:>2}   tzfix B-D7={x:>2}")
verdict = ("SKILL GAP — teaching the UTC idiom closes D7 (not paradigm-inherent)" if tt < tb
           else "PERSISTS — D7 survives the skill fix => genuinely SDP-structural" if tb > 0
           else "no baseline D7 (nothing to attribute — did the framework close the gap on its own?)")
print(f"    TOTAL: baseline B-D7={tb}  ->  tzfix B-D7={tt}\n  VERDICT: {verdict}")
print("  (4.1.0.dev4 baseline for reference: 7 -> 0)")
