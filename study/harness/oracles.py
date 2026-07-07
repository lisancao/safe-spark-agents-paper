"""Automated, arm-BLIND defect oracle grader (pre-reg §4/§5).

This is the instrument's grader. It maps a completed agent run to the
pre-registered D1-D9 oracle outcomes with **no human grading** and **no
knowledge of which arm produced the run**.

Blindness is structural, not a promise: `grade_run()` takes a `RunOutcome`
(what the agent produced + its error logs) and a `TaskOracleSpec` (which defect
classes are in scope for the task) and returns a `GradeResult`. There is **no
`arm` parameter anywhere in this module**. `tests/test_oracles.py` asserts that.
The runner is responsible for never leaking the arm into the outcome it hands us.

Single source of oracle truth: the semantic quantifiers (D2/D6/D7/D8) are the
EXACT functions from `experiments/defect_battery/quantify.py` (imported, not
re-implemented), so the grader cannot drift from the registered E3 numbers
(D2=246, D7=275, D8=250 / $49,778.06, D6=0 latent on seed=42). The structural
oracles (D1/D4/D5) are error-signature matches on the analysis log.

Detection stage (pre-reg §5):
  * dry_run  - caught by the structural gate before any executor ran
  * runtime  - caught when the job executed and failed
  * never    - reached COMPLETED output while still corrupt  (=> silent_defect)
  * n/a      - the in-scope defect simply did not manifest on this run/seed
"""
from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# --- defect taxonomy (pre-reg §4 table) -------------------------------------
# class: structural (gate-detectable) | semantic | state
# quant_key: the quantify.py oracle for semantic classes, else None
# error_signatures: substrings that identify a structural catch in an analysis log
DEFECT_TAXONOMY: Dict[str, Dict[str, Any]] = {
    "D1": {"name": "missing_unresolved_column", "klass": "structural",
           "dry_run_detectable": True, "quant_key": None,
           "error_signatures": ["UNRESOLVED_COLUMN", "42703"]},
    "D2": {"name": "wrong_type_timestamp_misparse", "klass": "semantic",
           "dry_run_detectable": False, "quant_key": "d2", "error_signatures": []},
    "D3": {"name": "unwatermarked_dedup", "klass": "state",
           "dry_run_detectable": False, "quant_key": None, "error_signatures": []},
    "D4": {"name": "broken_dag_missing_upstream", "klass": "structural",
           "dry_run_detectable": True, "quant_key": None,
           "error_signatures": ["TABLE_OR_VIEW_NOT_FOUND", "42P01"]},
    "D5": {"name": "immutable_config_mutation", "klass": "structural",
           "dry_run_detectable": True, "quant_key": None,
           "error_signatures": ["CANNOT_MODIFY_CONFIG", "46110"]},
    "D6": {"name": "nondeterministic_dedup", "klass": "semantic",
           "dry_run_detectable": False, "quant_key": "d6", "error_signatures": []},
    "D7": {"name": "timezone_day_bucket", "klass": "semantic",
           "dry_run_detectable": False, "quant_key": "d7", "error_signatures": []},
    "D8": {"name": "absent_quarantine_silent_drop", "klass": "semantic",
           "dry_run_detectable": False, "quant_key": "d8", "error_signatures": []},
    "D9": {"name": "unbounded_state", "klass": "state",
           "dry_run_detectable": False, "quant_key": None, "error_signatures": []},
}

STRUCTURAL = {d for d, m in DEFECT_TAXONOMY.items() if m["klass"] == "structural"}
SEMANTIC = {d for d, m in DEFECT_TAXONOMY.items() if m["klass"] == "semantic"}
STATE = {d for d, m in DEFECT_TAXONOMY.items() if m["klass"] == "state"}


# --- single-source import of the registered quantifiers ---------------------
def _load_quantify():
    """Import experiments/defect_battery/quantify.py as the ONE oracle source."""
    here = os.path.dirname(os.path.abspath(__file__))
    qpath = os.path.normpath(
        os.path.join(here, "..", "..", "defect_battery", "quantify.py")
    )
    if not os.path.exists(qpath):
        raise FileNotFoundError(
            f"oracle source quantify.py not found at {qpath}; the grader refuses "
            "to re-implement the oracle to avoid drift from the E3 numbers."
        )
    spec = importlib.util.spec_from_file_location("defect_battery_quantify", qpath)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def quantify_defect(spark, quant_key: str, dataset_path: str):
    """Run a registered quantifier (d2/d6/d7/d8) over a dataset.

    Returns (rows_affected, detail). This is the reused E3 oracle, verbatim.
    """
    q = _load_quantify()
    if quant_key not in q.QUANT:
        raise ValueError(f"unknown quant_key {quant_key!r}; known: {sorted(q.QUANT)}")
    return q.QUANT[quant_key](spark, dataset_path)


# --- the normalized output the grader inspects ------------------------------
@dataclass
class OutputProfile:
    """Residual-corruption metrics measured on a run's COMPLETED output.

    These are the oracle-relevant facts about what the agent actually shipped,
    independent of how it got there. Each field is the count of rows in the
    *output* still exhibiting that defect class (0 == the agent mitigated it).
    The runner builds this by reading the materialized output table; the battery
    builds it by applying the unmitigated transform via the quantifiers; tests
    build it from the pilot DEVLOG output stats.
    """

    # residual corruption present in the shipped output, per defect class
    d2_misparsed_rows: int = 0            # event_time values mis-typed in output
    d6_ambiguous_keys_unhandled: int = 0  # nondeterministic dedup survivors
    d7_wrong_day_rows: int = 0            # rows bucketed to the wrong calendar day
    d8_dollars_dropped: float = 0.0       # $ silently excluded from the output sum
    d8_rows_dropped: int = 0
    # optional reconciliation evidence the runner may attach
    reconciles: Optional[bool] = None     # output sum == ground-truth sum
    extra: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_quantifiers(spark, dataset_path: str, defects: List[str]) -> "OutputProfile":
        """Build a profile by applying the UNMITIGATED quantifiers (battery mode).

        Used when grading a defect variant whose transform does NOT mitigate the
        defect: the residual corruption equals the quantifier's rows_affected.
        """
        p = OutputProfile()
        for d in defects:
            qk = DEFECT_TAXONOMY[d]["quant_key"]
            if not qk:
                continue
            affected, detail = quantify_defect(spark, qk, dataset_path)
            if d == "D2":
                p.d2_misparsed_rows = int(affected)
            elif d == "D6":
                p.d6_ambiguous_keys_unhandled = int(affected)
            elif d == "D7":
                p.d7_wrong_day_rows = int(affected)
            elif d == "D8":
                p.d8_rows_dropped = int(affected)
                p.d8_dollars_dropped = float(detail.get("dollars_silently_excluded_from_sum", 0.0))
        return p


@dataclass
class RunOutcome:
    """Everything the grader needs about ONE run -- and NOTHING that reveals the arm.

    The runner populates this from the captured logs / materialized output. It
    intentionally omits `arm`, `base_model`, `skills`, `dry_run_gate`, AND any
    pre-computed gate hint: the grader must DERIVE the detection stage itself from
    *where* an error signature actually surfaced (analysis_log vs runtime_log),
    which is an observed property of the run, not a label handed to it. There is
    deliberately no `structural_caught_stage` field -- a prior version carried one
    and it was gate-revealing; `tests/test_oracles.py` now asserts RunOutcome
    exposes no such field so the grader provably cannot key off it.
    """

    completed: bool                       # did the run reach COMPLETED / materialize output?
    analysis_log: str = ""                # concatenated analysis/dry-run log text
    runtime_log: str = ""                 # concatenated execution log text
    output: Optional[OutputProfile] = None         # residual metrics (None if never completed)


@dataclass
class TaskOracleSpec:
    """Which defect classes are in scope for a task (from TASKS.lock.json)."""

    task: str
    defects_in_scope: List[str]           # e.g. ["D1","D2","D7","D8"]


@dataclass
class GradeResult:
    silent_defect: bool
    defect_classes: List[str]             # classes silently present in COMPLETED output
    detection_stage: str                  # run-level summary: dry_run|runtime|never|n/a
    per_defect_detection: Dict[str, str]  # D-id -> dry_run|runtime|never|n/a
    detail: Dict[str, Any] = field(default_factory=dict)


def _structural_signature_in(log: str, defect: str) -> bool:
    sigs = DEFECT_TAXONOMY[defect]["error_signatures"]
    return any(s in (log or "") for s in sigs)


def grade_run(spec: TaskOracleSpec, outcome: RunOutcome) -> GradeResult:
    """Map a run to oracle outcomes. BLIND to arm by construction.

    Logic, per defect class in scope:
      structural (D1/D4/D5): if its error signature appears in the analysis log
        -> caught at dry_run; if in the runtime log -> caught at runtime; either
        way NOT silent. If the run completed and the signature is absent, the
        structural defect was not present/triggered (n/a).
      semantic (D2/D6/D7/D8): only meaningful if the run COMPLETED. If the
        shipped output still exhibits the corruption (residual count > 0), it is
        a SILENT defect (detection_stage = never). 0 residual == mitigated (n/a).
      state (D3/D9): not oracle-detectable here (runtime/cluster-scale only);
        recorded as n/a with a note (pre-reg §4 honest split).
    """
    per_defect: Dict[str, str] = {}
    silent_classes: List[str] = []
    detail: Dict[str, Any] = {}

    out = outcome.output
    for d in spec.defects_in_scope:
        meta = DEFECT_TAXONOMY[d]
        klass = meta["klass"]

        if klass == "structural":
            if _structural_signature_in(outcome.analysis_log, d):
                per_defect[d] = "dry_run"
            elif _structural_signature_in(outcome.runtime_log, d):
                per_defect[d] = "runtime"
            else:
                per_defect[d] = "n/a"
            continue

        if klass == "state":
            per_defect[d] = "n/a"
            detail[d] = "state defect: runtime/cluster-scale only, not graded offline (pre-reg §4)"
            continue

        # semantic
        if not outcome.completed or out is None:
            # never reached output -> the semantic corruption could not ship
            per_defect[d] = "n/a"
            continue
        residual = _semantic_residual(d, out)
        detail[d] = residual
        if residual["rows"] > 0:
            per_defect[d] = "never"        # shipped corrupt, silently
            silent_classes.append(d)
        else:
            per_defect[d] = "n/a"          # mitigated by the agent

    silent_defect = outcome.completed and len(silent_classes) > 0

    # run-level summary stage
    if silent_defect:
        detection_stage = "never"
    elif "dry_run" in per_defect.values():
        detection_stage = "dry_run"
    elif "runtime" in per_defect.values():
        detection_stage = "runtime"
    else:
        detection_stage = "n/a"

    return GradeResult(
        silent_defect=silent_defect,
        defect_classes=sorted(silent_classes),
        detection_stage=detection_stage,
        per_defect_detection=per_defect,
        detail=detail,
    )


def _semantic_residual(defect: str, out: OutputProfile) -> Dict[str, Any]:
    """Residual corruption for a semantic defect in the shipped output."""
    if defect == "D2":
        return {"rows": int(out.d2_misparsed_rows), "metric": "event_time_misparsed"}
    if defect == "D6":
        return {"rows": int(out.d6_ambiguous_keys_unhandled), "metric": "ambiguous_dedup_keys"}
    if defect == "D7":
        return {"rows": int(out.d7_wrong_day_rows), "metric": "wrong_day_bucket"}
    if defect == "D8":
        return {"rows": int(out.d8_rows_dropped),
                "dollars": float(out.d8_dollars_dropped),
                "metric": "dollars_dropped_from_sum"}
    return {"rows": 0, "metric": "unknown"}
