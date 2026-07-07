#!/usr/bin/env python3
"""Stage corpus v3.1 (add new_lineitem_reconcile) and validate WITHOUT touching the
live TASKS.lock.json. Mirrors the checks in tests/test_corpus.py + test_complexity.py
against the staged copy, using the REAL harness.complexity + quantify_* registries.
Run from the study dir: python3 <this>.
"""
import json, os, sys, importlib
STUDY = "/home/lnc/repos/ssa-powered-run/experiments/safe_agent_study"
BATTERY = "/home/lnc/repos/ssa-powered-run/experiments/defect_battery"
OUT = "/tmp/claude-1000/-home-lnc-repos/eab86376-226f-4245-8cee-66e203dae70d/scratchpad/corpus23"
sys.path.insert(0, STUDY)
sys.path.insert(0, BATTERY)

from harness.complexity import score, bin_of  # the real scorer

# ---- the new task entry (mirrors orders_silver_gold / p12 structure) ----
NEW = {
    "id": "new_lineitem_reconcile",
    "title": "Line-item order-revenue mart with dedup and cross-stage reconciliation",
    "domain": "lineitem_reconcile",
    "substrate": "orders",
    "input": "infra/gen_messy_orders.py",
    "spec_ref": "agent-authored",
    "defects_in_scope": ["D1", "D5", "D6", "D7", "D8"],
    "oracles": {"D6": "quantify.d6", "D7": "quantify.d7", "D8": "quantify.d8"},
    "prompt": (
        "Finance is disputing our daily order revenue. Two problems keep surfacing: some "
        "orders are itemized — a list of line items, each with its own quantity and price "
        "— and our headline revenue for those days comes in low; and when an order is "
        "re-sent it lands in the numbers twice on some days and vanishes on others, so the "
        "same day totals differently each time we reprocess. We need daily revenue we can "
        "trust: every order counted once, itemized orders valued at their true line-item "
        "total, amounts and dates handled consistently, and the figures must tie out — the "
        "published daily total must equal the revenue of the orders we actually kept, and "
        "every order we received must be either counted or set aside for review with a "
        "reason, never silently lost. Reprocessing a day must reproduce the same totals.\n"
        "Output contract: table `gold_daily(event_date DATE [UTC calendar day], revenue, "
        "order_id, amount, category)`; table `silver_orders` keyed by `order_id` carrying "
        "`amount, category`; a rejected-records table with a reason column; the kept plus "
        "rejected records must reconcile to what was received, and the daily revenue must "
        "tie to the kept orders' line-item totals."
    ),
    "output_contract": {
        "table": "gold_daily",
        "revenue_col": "revenue",
        "date_col": "event_date",
        "substrate": "orders",
        "dedup_table": "silver_orders",
        "key_col": "order_id",
        "payload_cols": ["amount", "category"],
    },
    "complexity_score": 36,
    "complexity_bin": "High",
    "complexity_axes": {"A1": 3, "A2": 2, "A3": 2, "A4": 2, "A5": 3, "A6": 1, "A7": 3, "A8": 0},
    "input_args": ["--v3"],
    "graded_by": "output_oracle",
}

problems = []

# ---- 1. build staged lock from the live one ----
live = json.load(open(os.path.join(STUDY, "TASKS.lock.json")))
staged = json.loads(json.dumps(live))  # deep copy
if any(t["id"] == NEW["id"] for t in staged["tasks"]):
    problems.append("id already exists in live lock")
staged["tasks"].append(NEW)
staged["version"] = "3.0.0-corpus23"
staged["corpus_status"]["locked_n_tasks"] = len(staged["tasks"])
staged["corpus_status"]["complexity_distribution"]["High"] += 1
# coverage_matrix: increment each in-scope D-class
for dcode in NEW["defects_in_scope"]:
    staged["coverage_matrix"][dcode] += 1

# ---- 2. validate: complexity arithmetic (test_complexity.py) ----
s = score(NEW["complexity_axes"])
if s != NEW["complexity_score"]:
    problems.append(f"complexity_score {NEW['complexity_score']} != computed {s}")
if bin_of(s) != NEW["complexity_bin"]:
    problems.append(f"complexity_bin {NEW['complexity_bin']} != computed {bin_of(s)}")
if not all(k in NEW["complexity_axes"] for k in [f"A{i}" for i in range(1, 9)]):
    problems.append("complexity_axes missing an A1..A8 key")

# ---- 3. corpus_status / coverage consistency (test_corpus.py) ----
if staged["corpus_status"]["locked_n_tasks"] != len(staged["tasks"]):
    problems.append("locked_n_tasks != len(tasks)")
cd = staged["corpus_status"]["complexity_distribution"]
if cd["Low"] + cd["Med"] + cd["High"] != len(staged["tasks"]):
    problems.append("complexity_distribution sum != n_tasks")
from collections import Counter
actual = Counter()
for t in staged["tasks"]:
    for x in t["defects_in_scope"]:
        actual[x] += 1
for dcode, published in staged["coverage_matrix"].items():
    if dcode.startswith("D"):
        if published != actual[dcode]:
            problems.append(f"coverage_matrix[{dcode}]={published} != actual {actual[dcode]}")
        if actual[dcode] < 5:
            problems.append(f"class {dcode} in only {actual[dcode]} tasks (<5)")

# ---- 4. unique ids + non-empty prompt + D-codes valid ----
ids = [t["id"] for t in staged["tasks"]]
if len(ids) != len(set(ids)):
    problems.append("duplicate task id")
if not NEW["prompt"].strip():
    problems.append("empty prompt")
for d in NEW["defects_in_scope"]:
    if d not in {f"D{i}" for i in range(1, 10)}:
        problems.append(f"bad defect code {d}")

# ---- 5. semantic defects have a resolvable quantifier (test_semantic_defects...) ----
SEMANTIC = {"D2", "D6", "D7", "D8"}
MODMAP = {"quantify": ("quantify", "QUANT"), "quantify_ext": ("quantify_ext", "QUANT_EXT"),
          "quantify_hc": ("quantify_hc", "QUANT_HC"), "quantify_udf": ("quantify_udf", "QUANT_UDF")}
for d in NEW["defects_in_scope"]:
    if d in SEMANTIC:
        ref = NEW["oracles"].get(d)
        if not ref:
            problems.append(f"semantic {d} has no oracle entry")
            continue
        mod, key = ref.split(".", 1)
        try:
            m = importlib.import_module(MODMAP[mod][0])
            reg = getattr(m, MODMAP[mod][1])
            if key not in reg:
                problems.append(f"oracle {ref}: key '{key}' not in {MODMAP[mod][1]}")
        except Exception as e:  # noqa
            problems.append(f"oracle {ref} unresolvable: {type(e).__name__}: {e}")

# ---- 6. output_contract column requirements (test_semantic_tasks_have_output_contract) ----
oc = NEW["output_contract"]
need = {"D2": "date_col", "D7": "date_col", "D8": "revenue_col", "D6": "key_col"}
if "table" not in oc:
    problems.append("output_contract missing 'table'")
if oc.get("substrate") not in {"orders", "cdc", "payments"}:
    problems.append(f"output-oracle substrate {oc.get('substrate')} not in orders/cdc/payments")
for d in NEW["defects_in_scope"]:
    col = need.get(d)
    if col and not oc.get(col):
        problems.append(f"defect {d} needs output_contract['{col}'] (missing)")

# ---- 7. graded_by consistency (invariant substrates must be 'invariants') ----
INV_SUB = {"trades", "clickstream", "emails"}
if oc.get("substrate") in INV_SUB and NEW["graded_by"] != "invariants":
    problems.append("invariant substrate must be graded_by 'invariants'")
if NEW["graded_by"] not in {"output_oracle", "invariants"}:
    problems.append("graded_by not in enum")

# ---- 8. input generator exists ----
if not os.path.exists(os.path.join("/home/lnc/repos/ssa-powered-run", NEW["input"])):
    problems.append(f"generator {NEW['input']} not found")

# ---- report + write staged artifacts ----
os.makedirs(OUT, exist_ok=True)
json.dump(NEW, open(os.path.join(OUT, "new_lineitem_reconcile.task.json"), "w"), indent=2)
json.dump(staged, open(os.path.join(OUT, "TASKS.lock.v3.1.staged.json"), "w"), indent=1)

print("=== staged coverage_matrix (after add) ===")
print({k: v for k, v in staged["coverage_matrix"].items() if k.startswith("D")})
print("=== complexity:", s, bin_of(s))
print("=== corpus_status:", staged["corpus_status"]["locked_n_tasks"], "tasks;",
      staged["corpus_status"]["complexity_distribution"])
print()
if problems:
    print(f"VALIDATION: {len(problems)} PROBLEM(S):")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("VALIDATION: ALL CHECKS PASSED (staged v3.1 is corpus-test-clean)")
print("staged files ->", OUT)
