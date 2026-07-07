#!/usr/bin/env python3
"""Reproduce the §4.1.1 silent-defect composition: per-arm × per-semantic-class ships (detection=never),
plus task clustering. Usage: analyze_defect_composition.py [results.jsonl]  (default: the final set)."""
import json, os, sys, collections
HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.normpath(os.path.join(HERE, ".."))
path = sys.argv[1] if len(sys.argv) > 1 else next(
    (os.path.join(STUDY, f) for f in ("results.powered.AB.n12.final.jsonl", "results.powered.final.jsonl")
     if os.path.exists(os.path.join(STUDY, f))), None)
if not path or not os.path.exists(path):
    sys.exit("No results file found — run the primary sweep first (see repro/REPRODUCE.md §1), "
             "or pass a results.jsonl path as arg 1.")
rows = [json.loads(l) for l in open(path) if l.strip()]
SEM = ["D2", "D6", "D7", "D8"]
NAME = {"D2": "timestamp misparse", "D6": "nondeterministic dedup",
        "D7": "timezone/day-bucket", "D8": "silent row-drop / bad currency"}
def tid(r): return r.get("task") or r.get("task_id")
def ships(a, D): return sum(1 for r in rows if r.get("arm") == a
                            and r.get("per_defect_detection", {}).get(D) == "never")

print(f"=== silent-defect composition ({os.path.basename(path)}, spark {__import__('pyspark').__version__}) ===")
print(f"{'class':34}{'A':>6}{'B':>6}   read")
for D in SEM:
    a, b = ships("A", D), ships("B", D)
    read = "wash" if a and b and abs(a - b) <= 2 else ("SDP-specific" if b > a else "A-worse" if a > b else "")
    print(f"  {D} {NAME[D]:30}{a:>6}{b:>6}   {read}")
print("\n  per-class task clustering (arm B ships, where B>A):")
for D in SEM:
    bt = collections.Counter(tid(r) for r in rows if r.get("arm") == "B"
                             and r.get("per_defect_detection", {}).get(D) == "never")
    at = collections.Counter(tid(r) for r in rows if r.get("arm") == "A"
                             and r.get("per_defect_detection", {}).get(D) == "never")
    hot = [(t, at.get(t, 0), bt[t]) for t in bt if bt[t] > at.get(t, 0)]
    if hot:
        print(f"    {D}: " + ", ".join(f"{t}(A={a},B={b})" for t, a, b in sorted(hot, key=lambda x: -x[2])))
