#!/usr/bin/env python3
"""Section 1 supplementary analysis: what KINDS of hallucinations show up in the imperative arm (A/A2)
vs the SDP arm (B/B1), and WHERE they are caught (cheap dry-run gate vs runtime execute).

Reads the committed raw dump (study/raw/raw_20260628/all_results.jsonl). Each run records, per
iteration, the structured error_class at the `gate` (SDP dry-run) and `execute` (runtime) stages, plus
the final authored program. We classify error_classes into hallucination categories and split by arm
family and detection stage. Reproduce: `python3 study/analysis/hallucination_taxonomy.py`.

Note: a "hallucination" here = the agent inventing something that does not exist or writing code for
the wrong paradigm (a nonexistent path/table/column/attribute, or an imperative construct inside a
declarative pipeline). Pure logic/data defects (silent-defect D1-D9, ambiguous joins, bad casts) are
NOT counted as hallucinations; they are the study's separate correctness axis.
"""
import json, os, re
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, "..", "raw", "raw_20260628", "all_results.jsonl")
FAM = {"A": "imperative", "A2": "imperative", "B": "sdp", "B1": "sdp", "B2": "imperative_core"}

# error_class (SQLSTATE stripped) -> (hallucination_category, which paradigm it is native to)
HALLU = {
    "OUTPUT_PATH_NOT_FOUND":                     ("invented I/O path", "imperative"),
    "REQUIRED_OUTPUT_TABLE_NOT_FOUND":           ("invented I/O path", "imperative"),
    "ATTRIBUTE_NOT_SUPPORTED":                   ("invented / unsupported API", "imperative"),
    "SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.SET_RUNTIME_CONF": ("wrong-paradigm: imperative session control in a declarative pipeline", "sdp"),
    "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION": ("wrong-paradigm: eager action inside a declarative query function", "sdp"),
    "TABLE_OR_VIEW_NOT_FOUND":                    ("invented / undeclared table or view", "sdp"),
    "UNRESOLVED_COLUMN.WITH_SUGGESTION":          ("invented column name", "both"),
    "UNRESOLVED_COLUMN":                          ("invented column name", "both"),
}
# error_classes that are logic/data defects, NOT hallucinations (excluded from the hallucination counts)
NON_HALLU = {"AMBIGUOUS_REFERENCE", "CAST_INVALID_INPUT", "STREAM_FAILED", "NO_CODE_PRODUCED",
             "RuntimeError", "ValueError", "AnalysisException", "JJavaError", "IllegalArgumentException",
             "CANNOT_PARSE_TIMESTAMP"}

def norm(ec): return ec.split(" (SQLSTATE")[0] if ec else ec

def main():
    rows = [json.loads(l) for l in open(RAW) if l.strip()]
    tot = Counter(FAM.get(r["arm"]) for r in rows)

    # (family, stage) -> Counter(error_class)   [iteration occurrences]
    grid = defaultdict(Counter)
    runs_with = defaultdict(lambda: defaultdict(set))
    for r in rows:
        fm = FAM.get(r["arm"]); rid = r.get("run_id")
        for it in (r.get("per_iteration") or []):
            for stage in ("gate", "execute"):
                ec = norm((it.get(stage) or {}).get("error_class"))
                if ec:
                    grid[(fm, stage)][ec] += 1
                    runs_with[ec][fm].add(rid)

    print("runs by arm family:", dict(tot))
    print("\n== WHERE errors are caught (iteration counts) ==")
    for fm in ("imperative", "sdp"):
        g = sum(grid[(fm, "gate")].values()); e = sum(grid[(fm, "execute")].values()); t = g + e or 1
        print(f"  {fm:10s} dry-run GATE = {g:4d} ({100*g//t:2d}%)   runtime EXECUTE = {e:4d} ({100*e//t:2d}%)")

    print("\n== HALLUCINATION taxonomy: runs affected, by arm family ==")
    cat = defaultdict(lambda: defaultdict(int))
    for ec, (category, _) in HALLU.items():
        for fm in ("imperative", "sdp"):
            cat[category][fm] += len(runs_with[ec][fm])
    print(f"  {'category':64s} imperative   sdp")
    for category in sorted(cat, key=lambda c: -(cat[c]['imperative'] + cat[c]['sdp'])):
        print(f"  {category:64s} {cat[category]['imperative']:6d}   {cat[category]['sdp']:5d}")

    print("\n== per error_class (iters gate/execute), by family ==")
    allec = sorted(set(e for (f, s), c in grid.items() for e in c), key=lambda e: -sum(grid[(f, s)][e] for f in ("imperative", "sdp") for s in ("gate", "execute")))
    for ec in allec:
        tag = "HALLU" if ec in HALLU else ("logic" if ec in NON_HALLU else "?")
        ig = grid[("imperative", "gate")][ec]; ie = grid[("imperative", "execute")][ec]
        sg = grid[("sdp", "gate")][ec]; se = grid[("sdp", "execute")][ec]
        print(f"  [{tag:5s}] {ec:52s} imp g{ig}/e{ie}   sdp g{sg}/e{se}")

    # example offending lines from the final authored program
    PAT = {
        "invented I/O path (imperative)": re.compile(r"\.(load|parquet|csv|save)\(|/tmp/|/mnt/|dbfs:"),
        "session control in declarative pipeline (sdp, gate-caught)": re.compile(r"[\w.]*\.conf\.set\("),
        "eager action in query function (sdp)": re.compile(r"[\w\]\)]\.(collect|show|first|toPandas)\(\s*\)"),
        "undeclared table / view (sdp)": re.compile(r"\.read\.table\(|spark\.table\("),
    }
    print("\n== example offending lines (from final authored programs) ==")
    seen = defaultdict(int)
    for r in rows:
        fm = FAM.get(r["arm"]); fp = r.get("final_program") or ""
        for name, rx in PAT.items():
            for l in fp.splitlines():
                s = l.strip()
                if rx.search(s) and s[:1] != "#" and "def " not in s and len(s) < 120:
                    if seen[(name, fm)] < 2:
                        seen[(name, fm)] += 1
                        print(f"  [{fm}/{r['arm']}] {name}\n      {r['task']}: {s}")
                    break

if __name__ == "__main__":
    main()
