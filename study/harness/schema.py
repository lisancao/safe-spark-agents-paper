"""Schema-stable results row + environment sidecar for the safe-agent study.

Every (task, arm, seed) run emits exactly one `ResultRow` as a JSON line in
`results.jsonl`. The schema is FROZEN here: the analysis layer
(`analysis/analyze.py`) and the pre-registration's reproducibility commitment
(§8) both depend on these field names. Adding a field is fine; renaming or
removing one is a breaking change and must be logged in `DEVIATIONS.md`.

Pre-reg §8 requires every run row to record: Spark version, image digest, git
SHA, base-model id, seed, executor config, wall time, executor-seconds, and
exit/error class. Those are all mandatory below; the remainder are the H1/H2
outcome fields and honesty controls.

This module deliberately has NO third-party dependencies so it can be imported
in any environment (grader, runner, analysis, tests).
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

SCHEMA_VERSION = "1.0.0"

# Detection stage of the (first) silent-capable defect, per pre-reg §5:
# the dry-run gate, a runtime/materialization failure, or never (silent).
DETECTION_STAGES = ("dry_run", "runtime", "never", "n/a")

# Closed vocabulary for exit_class so the analysis layer can group cleanly.
# The lowercase classes are agent/loop OUTCOMES; the UPPER_CASE PROPOSE_*/HARNESS_*
# classes are CRASH-SAFETY outcomes -- a cell that failed soft (was recorded and the
# sweep continued) rather than producing a normal agent result. Adding members is
# additive (existing rows stay valid); keep in lockstep with backends.base
# ProposeError.exit_class and RESULTS_JSON_SCHEMA below.
EXIT_CLASSES = (
    "completed",        # reached COMPLETED / materialized output
    "analysis_error",   # failed structural analysis (dry-run / eager analysis)
    "runtime_error",    # failed during execution on the cluster
    "max_iterations",   # hit the iteration cap without a green output
    "harness_error",    # the harness itself failed (not an agent outcome)
    # --- propose-path crash-safety (the cell failed soft and was recorded) -------
    "PROPOSE_TIMEOUT",    # brain.propose() exceeded the harness wall-clock bound (killed)
    "PROPOSE_API_ERROR",  # Anthropic API/SDK error after retries (or unusable output)
    "PROPOSE_RATE_LIMIT", # 429 / rate-limit that survived the client's retries
    "HARNESS_EXCEPTION",  # any other unexpected exception caught by the per-cell net
    # --- unified HARNESS-FAULT quarantine outcome (Part B harness-fault policy) ---
    # A cell that FAULTED (any HARNESS_FAULT_EXIT_CLASS below) AND faulted AGAIN on the
    # one allowed retry is QUARANTINED with this class: it is an INSTRUMENT failure, is
    # EXCLUDED from the H1-H4 statistics (analysis/analyze.py), and never accrues toward
    # max_iterations. The specific underlying fault is preserved in `harness_fault_reason`
    # for the paper's excluded-data (quarantine) appendix.
    "HARNESS_ERROR",
)

# The SINGLE source of truth for "this row is a HARNESS FAULT" (an INSTRUMENT failure,
# never an agent outcome). It unifies BOTH fault paths the policy must cover together:
#   (i)  the propose-call faults (#31): PROPOSE_TIMEOUT / PROPOSE_API_ERROR /
#        PROPOSE_RATE_LIMIT / HARNESS_EXCEPTION, and
#   (ii) the new SDP/infra instrument faults (Part A) which surface as HARNESS_EXCEPTION
#        (carried by backends.base.HarnessFault).
# `HARNESS_ERROR` itself (the quarantine bucket) is a fault too, so a re-loaded quarantine
# row is still recognized. The runner's circuit breaker and analyze.py both key off this
# set, so a propose-throttle on one batch and an SDP path break on another trip the SAME
# breaker. Keep in lockstep with backends.base.ProposeError / HarnessFault exit_class.
HARNESS_FAULT_EXIT_CLASSES = frozenset({
    "PROPOSE_TIMEOUT", "PROPOSE_API_ERROR", "PROPOSE_RATE_LIMIT",
    "HARNESS_EXCEPTION", "HARNESS_ERROR",
})


def is_harness_fault(exit_class: str) -> bool:
    """True iff `exit_class` denotes a HARNESS FAULT (instrument failure), per the
    unified SSOT above. Used by the runner's retry/quarantine/circuit-breaker policy
    and by the analysis layer's quarantine exclusion."""
    return exit_class in HARNESS_FAULT_EXIT_CLASSES


@dataclass
class ResultRow:
    """One row of results.jsonl. Field order here is the canonical order."""

    # --- identity / matching --------------------------------------------
    run_id: str                 # unique per (task, arm, seed, attempt)
    task: str                   # task id, must be a key in TASKS.lock.json
    arm: str                    # arm id (A / A2 / B / B1 / B2), key in arms/*.json
    seed: int                   # fixed integer seed from SEEDS.lock.json

    # --- pre-reg §8 reproducibility provenance --------------------------
    spark_version: str
    image_digest: str           # container image digest (sandbox), or "uncontainerized"
    git_sha: str                # repo commit the run was produced from
    base_model_id: str          # MUST be identical across arms (validator enforces)
    executor_config: Dict[str, Any]  # {instances, cores, mem, price_usd_per_executor_hour, ...}

    # --- H1 primary outcome (pre-reg §5) --------------------------------
    silent_defect: bool         # output reached COMPLETED with >=1 oracle-detected defect
    defect_classes: List[str]   # D-ids that are silently present in the COMPLETED output
    detection_stage: str        # one of DETECTION_STAGES (earliest stage any defect was caught)

    # --- loop / cost (H2, pre-reg §5/§6) --------------------------------
    iterations: int             # total agent iterations in the loop
    wall_s: float               # total wall-clock seconds for the run
    executor_seconds: Optional[float]  # MEASURED executor-seconds (stage-diff); None if never measured (see executor_seconds_wallclock)
    usd: float                  # total USD cost (priced on measured executor_seconds when present, else the wall-clock cross-check)
    exit_class: str             # one of EXIT_CLASSES (final loop outcome)

    # --- secondary / honesty controls (pre-reg §5) ----------------------
    task_success: bool = False              # produced a COMPLETED/materialized output at all
    reached_correct: bool = True            # False -> executor_seconds_to_correct is the ITT imputation (B9)
    iterations_to_green: Optional[int] = None       # iters to first correct output (None if never)
    wall_s_to_green: Optional[float] = None
    executor_seconds_to_correct: Optional[float] = None  # compute-to-correct (H2)
    # --- D-5: three compute measurements reported side by side -----------------
    # `executor_seconds` (above) is the AUTHORITATIVE measured stage-diff (else
    # wall-derived). `cpu_seconds` is the measured CPU-seconds (stage executorCpuTime);
    # `executor_seconds_wallclock` is the wall x instances x busy_fraction cross-check
    # carried ALWAYS so H2 can report effects across measurements.
    cpu_seconds: Optional[float] = None                  # measured CPU-seconds (None if unavailable)
    cpu_seconds_to_correct: Optional[float] = None       # measured CPU-seconds up to green
    executor_seconds_wallclock: float = 0.0              # wall x slots cross-check (total)
    executor_seconds_wallclock_to_correct: Optional[float] = None  # cross-check up to green
    dry_run_intercepts: int = 0             # # failing iterations caught at the dry-run gate ($0)
    failing_iterations: int = 0             # # iterations whose proposal failed (gate OR runtime)
    per_defect_detection: Dict[str, str] = field(default_factory=dict)  # D-id -> stage
    per_iteration: List[Dict[str, Any]] = field(default_factory=list)   # exit/error class per iter

    # --- H5 conciseness: the FINAL ACCEPTED program + its size (pre-reg addendum K/L)
    # `final_program` is the agent-authored source that reached COMPLETED (None if the
    # run never completed); for SDP arms the transformations @dp module, for imperative
    # arms the pipeline.py program. The harness-emitted SDP spark-pipeline.yml is NOT
    # part of it. The four size fields are computed by harness/program_metrics.py and
    # are None whenever final_program is None (or unparseable). `*_body` excludes the
    # mandatory import/decorator/def-header scaffolding so declarative is not penalised
    # for the @dp wrapper it is required to write.
    final_program: Optional[str] = None
    final_program_loc: Optional[int] = None          # non-blank, non-comment source lines (raw)
    final_program_loc_body: Optional[int] = None     # LOC excluding import/decorator/def-header scaffolding
    ast_node_count: Optional[int] = None             # total ast.walk nodes (raw)
    ast_node_count_body: Optional[int] = None        # ast nodes excluding the scaffolding subtrees

    # --- token accounting (cost/validity audit, pre-reg §8) -------------
    # Total LLM tokens the brain consumed across the episode's iterations (sum of the
    # per-iteration usage now also recorded in `per_iteration[i]["tokens"]`). Persisted
    # so per-cell cost and the harness-fault validity audit are reconstructable from the
    # artifacts alone. 0 for backends with no LLM (replay/scripted).
    input_tokens: int = 0
    output_tokens: int = 0

    # --- harness-fault quarantine (Part B) ------------------------------
    # Set ONLY on a quarantined cell (exit_class == HARNESS_ERROR): the specific
    # underlying fault class (PROPOSE_TIMEOUT / PROPOSE_API_ERROR / PROPOSE_RATE_LIMIT /
    # HARNESS_EXCEPTION) preserved for the quarantine report's `reason` column. None on
    # every non-quarantined row.
    harness_fault_reason: Optional[str] = None

    # --- provenance / audit --------------------------------------------
    backend: str = "unknown"          # agent backend that drove the loop (replay / anthropic / omnigent)
    transcript_path: Optional[str] = None  # path to the captured full transcript
    schema_version: str = SCHEMA_VERSION
    timestamp_utc: Optional[str] = None    # stamped at write time (passed in; no Date.now here)
    notes: Optional[str] = None
    # Structured crash detail for harness_error rows: the exception TYPE + MESSAGE that
    # escaped the cell (e.g. "SparkException: Only one SparkContext should be running ...").
    # Kept SEPARATE from `notes` (which truncates) so a crash is machine-greppable, not
    # buried in prose. Empty string for non-crash rows; backward-compatible (optional).
    error: Optional[str] = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=False)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ResultRow":
        known = {f.name for f in dataclasses.fields(ResultRow)}
        return ResultRow(**{k: v for k, v in d.items() if k in known})


def validate_row(d: Dict[str, Any]) -> List[str]:
    """Return a list of human-readable problems with a row dict; [] == valid.

    Hand-rolled (no jsonschema dependency) so it runs anywhere. Mirrors
    results_schema.json, which is the published, machine-readable contract.
    """
    problems: List[str] = []
    required = {
        "run_id": str, "task": str, "arm": str, "seed": int,
        "spark_version": str, "image_digest": str, "git_sha": str,
        "base_model_id": str, "executor_config": dict,
        "silent_defect": bool, "defect_classes": list, "detection_stage": str,
        "iterations": int, "wall_s": (int, float),
        "usd": (int, float), "exit_class": str,
    }
    # executor_seconds is the MEASURED surface: NULLABLE and NOT required (None == no
    # live metric). The always-present executor-seconds figure pre-reg §8 requires is
    # the wall-clock cross-check `executor_seconds_wallclock` (see D-5), which IS
    # required. Keep this in lockstep with results_schema.json / RESULTS_JSON_SCHEMA.
    if "executor_seconds" in d and d["executor_seconds"] is not None and (
            not isinstance(d["executor_seconds"], (int, float)) or isinstance(d["executor_seconds"], bool)):
        problems.append(f"field executor_seconds has wrong type: {type(d['executor_seconds']).__name__}")
    if "executor_seconds_wallclock" not in d:
        problems.append("missing required field: executor_seconds_wallclock")
    elif not isinstance(d["executor_seconds_wallclock"], (int, float)) or isinstance(d["executor_seconds_wallclock"], bool):
        problems.append(f"field executor_seconds_wallclock has wrong type: {type(d['executor_seconds_wallclock']).__name__}")
    for k, typ in required.items():
        if k not in d:
            problems.append(f"missing required field: {k}")
            continue
        if isinstance(typ, tuple):
            if not isinstance(d[k], typ) or isinstance(d[k], bool) and float not in typ:
                problems.append(f"field {k} has wrong type: {type(d[k]).__name__}")
        elif not isinstance(d[k], typ) or (typ is int and isinstance(d[k], bool)):
            problems.append(f"field {k} has wrong type: {type(d[k]).__name__}")
    # H5 conciseness fields are nullable integers (None until a run completes). Validate
    # type only when present and non-None; mirrors results_schema.json.
    for k in ("final_program_loc", "final_program_loc_body", "ast_node_count",
              "ast_node_count_body"):
        if k in d and d[k] is not None and (
                not isinstance(d[k], int) or isinstance(d[k], bool)):
            problems.append(f"field {k} has wrong type: {type(d[k]).__name__}")
    if "final_program" in d and d["final_program"] is not None and not isinstance(d["final_program"], str):
        problems.append(f"field final_program has wrong type: {type(d['final_program']).__name__}")
    # `error` is the structured crash detail on harness_error rows: a nullable string
    # (empty for non-crash rows). Validate type only when present and non-None; mirrors
    # results_schema.json (["string", "null"]).
    if "error" in d and d["error"] is not None and not isinstance(d["error"], str):
        problems.append(f"field error has wrong type: {type(d['error']).__name__}")
    if d.get("detection_stage") not in DETECTION_STAGES:
        problems.append(f"detection_stage not in {DETECTION_STAGES}: {d.get('detection_stage')!r}")
    if d.get("exit_class") not in EXIT_CLASSES:
        problems.append(f"exit_class not in {EXIT_CLASSES}: {d.get('exit_class')!r}")
    if d.get("silent_defect") and not d.get("defect_classes"):
        problems.append("silent_defect is true but defect_classes is empty")
    if d.get("silent_defect") is False and d.get("defect_classes"):
        # A non-silent run may still have caught-and-fixed defects, but a COMPLETED
        # run with a populated defect_classes list and silent_defect False is a
        # contradiction the grader should never emit.
        if d.get("exit_class") == "completed":
            problems.append("completed run lists defect_classes but silent_defect is false")
    return problems


# JSON-Schema document (Draft-07-ish) published alongside the code as the
# machine-readable contract. Kept in sync with validate_row / ResultRow by hand.
RESULTS_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "safe_agent_study results.jsonl row",
    "type": "object",
    "required": [
        "run_id", "task", "arm", "seed", "spark_version", "image_digest",
        "git_sha", "base_model_id", "executor_config", "silent_defect",
        "defect_classes", "detection_stage", "iterations", "wall_s",
        "executor_seconds_wallclock", "usd", "exit_class",
    ],
    "properties": {
        "run_id": {"type": "string"},
        "task": {"type": "string"},
        "arm": {"type": "string", "enum": ["A", "A2", "B", "B1", "B2"]},
        "seed": {"type": "integer"},
        "spark_version": {"type": "string"},
        "image_digest": {"type": "string"},
        "git_sha": {"type": "string"},
        "base_model_id": {"type": "string"},
        "executor_config": {"type": "object"},
        "silent_defect": {"type": "boolean"},
        "defect_classes": {"type": "array", "items": {"type": "string"}},
        "detection_stage": {"type": "string", "enum": list(DETECTION_STAGES)},
        "iterations": {"type": "integer", "minimum": 0},
        "wall_s": {"type": "number", "minimum": 0},
        "executor_seconds": {"type": ["number", "null"], "minimum": 0},
        "usd": {"type": "number", "minimum": 0},
        "exit_class": {"type": "string", "enum": list(EXIT_CLASSES)},
        "task_success": {"type": "boolean"},
        "reached_correct": {"type": "boolean"},
        "iterations_to_green": {"type": ["integer", "null"]},
        "wall_s_to_green": {"type": ["number", "null"]},
        "executor_seconds_to_correct": {"type": ["number", "null"]},
        "cpu_seconds": {"type": ["number", "null"]},
        "cpu_seconds_to_correct": {"type": ["number", "null"]},
        "executor_seconds_wallclock": {"type": "number", "minimum": 0},
        "executor_seconds_wallclock_to_correct": {"type": ["number", "null"]},
        "dry_run_intercepts": {"type": "integer", "minimum": 0},
        "failing_iterations": {"type": "integer", "minimum": 0},
        "per_defect_detection": {"type": "object"},
        "per_iteration": {"type": "array"},
        "final_program": {"type": ["string", "null"]},
        "final_program_loc": {"type": ["integer", "null"], "minimum": 0},
        "final_program_loc_body": {"type": ["integer", "null"], "minimum": 0},
        "ast_node_count": {"type": ["integer", "null"], "minimum": 0},
        "ast_node_count_body": {"type": ["integer", "null"], "minimum": 0},
        "input_tokens": {"type": "integer", "minimum": 0},
        "output_tokens": {"type": "integer", "minimum": 0},
        "harness_fault_reason": {"type": ["string", "null"]},
        "backend": {"type": "string"},
        "transcript_path": {"type": ["string", "null"]},
        "schema_version": {"type": "string"},
        "timestamp_utc": {"type": ["string", "null"]},
        "notes": {"type": ["string", "null"]},
        "error": {"type": ["string", "null"]},
    },
    "additionalProperties": False,
}
