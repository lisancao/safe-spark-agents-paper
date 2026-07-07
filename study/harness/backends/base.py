"""Pluggable agent backend interfaces (the core reproducible mechanism).

The runner owns the loop, the cost model, and the blind grading. The backend
owns only the two things that genuinely differ between "real" and "offline":

  * AgentBrain   -- proposes the next transform code + command (the LLM step).
  * SparkExecutor -- runs the structural gate / executes on the cluster and
                     returns measured outcomes (exit, error class, timings,
                     executor-seconds).

Separating these keeps the EXPERIMENT controls in the runner: the same loop,
the same cost accounting, and the same grader are applied to every arm and every
backend. A backend cannot fake the comparison because it never sees the arm
label as anything other than the manifest's loop fields, and it cannot invent
costs the runner doesn't measure.

Two concrete backend pairs ship:
  * live  (anthropic AgentBrain + Connect SparkExecutor) -- the real loop. Needs
    an API key and a reachable Spark Connect backend; see live.py.
  * replay (scripted AgentBrain + deterministic SparkExecutor) -- replays a
    recorded episode trace with NO LLM, NO Spark, NO network, so the full
    pipeline (loop -> cost -> grade -> results.jsonl -> stats) is validatable
    offline. See replay.py.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

# Hard wall-clock bound on a single agent-code execution, so a hung/streaming agent
# program (e.g. foreachBatch + awaitTermination, which never terminates on a bounded
# batch input) can NEVER wedge the run. On timeout the iteration fails gracefully with
# error_class EXECUTION_TIMEOUT and the loop continues.
#   * IN-PROCESS imperative exec (LocalSparkExecutor): tight bound -- ample for a
#     bounded job on the tiny study dataset, short enough to fail a hung query fast.
AGENT_EXEC_TIMEOUT_S = 150
#   * SUBPROCESS CLI/command paths (ConnectExecutor `_run`: SDP CLI + imperative
#     spark-submit/python over Connect): more headroom for cluster scheduling + the
#     per-invocation JVM startup, still a hard bound (the child process GROUP is killed
#     on timeout, so it cannot leak).
CONNECT_CMD_TIMEOUT_S = 600
# Hard bound on each POST-EXECUTION in-process Spark/py4j step (completion read-back,
# executor-seconds UI snapshot, session interrupt/stop). The AGENT_EXEC_TIMEOUT_S above
# only guards the agent program's worker thread; the steps that run AFTER it returns
# (read-back, profiling, SparkSession.stop) issue blocking py4j calls that hang FOREVER
# if the in-process JVM wedges (the live calibration hang: outputs materialized, then no
# progress). Bounding each of those with this short cap turns a wedged JVM into a fast,
# clean EXECUTION_TIMEOUT row instead of an indefinite stall. These run on the tiny study
# dataset, so a healthy step finishes in well under a second.
POST_EXEC_TIMEOUT_S = 30
# error class stamped on a timed-out execution (extracted from the bracketed log token).
EXECUTION_TIMEOUT_CLASS = "EXECUTION_TIMEOUT"

# HARNESS-OWNED hard wall-clock bound on a single brain.propose() (the Anthropic
# Messages call), enforced by the runner/brain via a KILLABLE subprocess -- the real
# fix, independent of the SDK's own timeout. The Anthropic client is built with
# timeout=120 + max_retries=6 (live.py), whose worst-case retry budget is ~14 min; this
# bound sits ABOVE that so it only fires when the SDK timeout MISBEHAVES (a stuck socket
# that never honors the per-attempt timeout). On expiry the child process GROUP is
# SIGKILLed and the cell fails soft with exit_class PROPOSE_TIMEOUT -- a hung call can
# never wedge the multi-hour/multi-day serial sweep.
PROPOSE_WALL_TIMEOUT_S = 1200


# ---------------------------------------------------------------------------
# Propose-path failures (crash-safety): every brain.propose() failure mode is
# converted into one of these so the runner can record a STRUCTURED failed cell and
# CONTINUE the sweep instead of aborting the whole batch. Each carries the schema
# `exit_class` to stamp on the ResultRow (kept in lockstep with schema.EXIT_CLASSES).
# Defined here (dependency-free) so the runner can catch them without importing
# anthropic or any live-only module.
# ---------------------------------------------------------------------------
class ProposeError(Exception):
    """A brain.propose() failure the harness converts into a soft-failed cell.

    Subclasses set `exit_class` to the schema exit_class recorded on the row. The
    base class is the catch-all (an unexpected propose failure) -> HARNESS_EXCEPTION.
    """

    exit_class = "HARNESS_EXCEPTION"


class ProposeTimeout(ProposeError):
    """propose() exceeded the harness-owned wall-clock bound (the child group was
    SIGKILLed). The hang is bounded and the cell fails soft."""

    exit_class = "PROPOSE_TIMEOUT"


class ProposeApiError(ProposeError):
    """propose() raised an Anthropic API/SDK error (or returned unusable output)
    after the client's own retries were exhausted."""

    exit_class = "PROPOSE_API_ERROR"


class ProposeRateLimited(ProposeApiError):
    """A 429 / rate-limit error that survived the client's retries -- classified
    distinctly from a generic API error so the sweep's failure mix is legible."""

    exit_class = "PROPOSE_RATE_LIMIT"


# ---------------------------------------------------------------------------
# Infrastructure / instrument failures (the OTHER half of the unified
# HARNESS-FAULT notion -- see schema.HARNESS_FAULT_EXIT_CLASSES and the policy in
# runner.main). A HarnessFault is raised when the INSTRUMENT is broken (an SDP spec
# the harness was supposed to materialize is missing, a doubled/relative path the
# CLI cannot resolve, a required materialized file absent) -- NOT when the agent's
# code failed. It deliberately is NOT a ProposeError (those are the LLM-call faults);
# both families share the single `exit_class` vocabulary so the runner's per-cell net
# and the circuit breaker treat them identically. It carries `exit_class
# = HARNESS_EXCEPTION` so an uncaught HarnessFault flows through the existing
# _run_cell_safe net as a recognized harness fault, while `reason` preserves the
# specific instrument failure for the quarantine report.
# ---------------------------------------------------------------------------
class HarnessFault(Exception):
    """An INSTRUMENT failure (broken harness), never an agent outcome. Raised by the
    SDP spec/file existence guards (live.py / runner._materialize_proposal). Recognized
    as a harness fault by the per-cell net and the circuit breaker; a broken instrument
    can therefore never masquerade as an agent result."""

    exit_class = "HARNESS_EXCEPTION"

    def __init__(self, message: str, reason: str = "HARNESS_FAULT"):
        super().__init__(message)
        # a short, stable token for the quarantine report's `reason` column, e.g.
        # SDP_SPEC_MISSING / SDP_MATERIALIZATION_MISSING. Distinct from `exit_class`,
        # which is the schema bucket the row is filed under.
        self.reason = reason


@dataclass
class Proposal:
    """One agent turn: the code it wrote + the command it wants to run."""

    iteration: int
    code: str                       # the transform code the agent produced this turn
    command: str                    # the command it chose (must be in arm.allowed_commands)
    rationale: str = ""             # short note (for the transcript)
    # an opaque tag the offline executor uses to look up a deterministic outcome;
    # the LIVE executor ignores it (it actually runs the code).
    replay_tag: Optional[str] = None
    # the API stop_reason for this turn (live brain only), surfaced so a truncated
    # turn (`stop_reason="max_tokens"` -> often an empty code block) is VISIBLE in the
    # per-iteration record instead of silently producing a no-code iteration.
    stop_reason: Optional[str] = None
    # PER-TURN LLM token usage for THIS proposal (live brain only; 0 for replay/scripted).
    # The runner records these in the per-iteration transcript and sums them onto the
    # ResultRow so per-cell cost/validity is auditable from the persisted artifacts alone
    # (Part A.5). #31 already serialized usage back across the killable-subprocess
    # boundary; this carries that per-turn figure out to the row instead of leaving it
    # only in the brain's running totals.
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class GateOutcome:
    """Result of the structural dry-run gate (driver-only, no executors)."""

    failed: bool
    wall_s: float
    error_class: Optional[str] = None   # e.g. UNRESOLVED_COLUMN (SQLSTATE 42703)
    log: str = ""


@dataclass
class ExecOutcome:
    """Result of executing a proposal on the cluster."""

    failed: bool
    completed: bool                  # reached COMPLETED / materialized output
    wall_s: float
    executor_seconds: Optional[float] = None  # measured PER-ITERATION (stage-diff); None -> derive from wall
    cpu_seconds: Optional[float] = None       # measured PER-ITERATION CPU-seconds (stage-diff); None -> unavailable
    error_class: Optional[str] = None
    log: str = ""
    # Identifiers of the tables the agent MATERIALIZED this iteration. On a real
    # executor the runner reads these back (via read_table) and runs the task's
    # OUTPUT oracle to build the OutputProfile -- this is the authoritative live
    # grading path (B1). `output_metrics` below is ONLY a replay/offline shortcut
    # (canned residuals) and is never produced by a real executor.
    output_tables: List[str] = field(default_factory=list)
    output_metrics: Optional[Dict[str, Any]] = None


@dataclass
class LoopState:
    """Mutable state threaded through the loop and shown to the brain."""

    task: str
    seed: int
    workspace: str
    dataset_path: str                    # PRIMARY input the agent + oracle read (staged-remote on the live cluster)
    # ADDITIONAL declared inputs (corpus v3 multi-input tasks: stream-stream join,
    # as-of SCD2 join, HC-1, HC-2). Maps a neutral logical name -> the staged path,
    # parallel to dataset_path and staged IDENTICALLY across arms. Empty for the
    # single-input tasks, so their contract is byte-for-byte unchanged.
    aux_inputs: Dict[str, str] = field(default_factory=dict)
    output_table: str = "agent_output"   # logical output dataset name (table for SDP/Connect)
    output_path: str = ""                # local imperative parquet output path (AGENT_OUTPUT_PATH)
    # LOCAL IMPERATIVE D6 SECONDARY dedup table (when the task's dedup_table differs
    # from the primary table, e.g. silver_orders/clean_orders vs gold_daily): its own
    # on-disk parquet path (AGENT_DEDUP_PATH). Set ONLY for those tasks; "" otherwise
    # (same-table D6 already grades the primary from disk, and SDP/Connect keep their
    # live-catalog read). Mirrors output_path for the secondary table so its grade
    # survives the agent's idiomatic `spark.stop()`. Defect-neutral: a location only.
    dedup_path: str = ""
    output_storage: str = ""             # SDP spec storage base (cluster-reachable on the live cluster)

    @property
    def imperative_output_path(self) -> str:
        """Path the imperative LOCAL contract writes/reads. Defaults to a stable
        per-cell parquet directory under the workspace when the runner did not set
        one explicitly (keeps unit tests concise)."""
        return self.output_path or os.path.join(self.workspace, "agent_output.parquet")
    sdp_catalog: str = "spark_catalog"   # SDP spec catalog (proven-good on the live Connect endpoint)
    sdp_database: str = "default"        # SDP spec database (proven-good on the live Connect endpoint)
    feedback: List[str] = field(default_factory=list)   # errors fed back across iters
    history: List[Proposal] = field(default_factory=list)

    def add_feedback(self, msg: str) -> None:
        self.feedback.append(msg)


def aux_locations_text(aux_inputs: Dict[str, str]) -> str:
    """The agent-visible aux-input contract: STRICTLY location-only, and IDENTICAL
    for both paradigms (no env-var doc, no format, no how-to / 'read as ...' hint --
    that would be a prompt-no-leak violation). Reveals only the neutral input name
    and where it is, exactly mirroring how the primary 'Dataset: <path>' line works.
    Returns '' when the task declares no aux inputs."""
    aux = aux_inputs or {}
    if not aux:
        return ""
    lines = ["Additional input locations:"]
    lines += [f"  - {name}: {path}" for name, path in aux.items()]
    return "\n".join(lines)


def aux_input_env(state: "LoopState") -> Dict[str, str]:
    """Neutral env contract for a task's ADDITIONAL declared inputs (multi-input
    staging). The PRIMARY input is delivered as before via AGENT_INPUT_PATH; the
    rest are delivered as a JSON name->path map in AGENT_AUX_INPUTS plus an
    individual AGENT_AUX_INPUT_<NAME> per input (uppercased name) for ergonomics.

    Returns {} when the task declares no aux inputs, so single-input arms get the
    EXACT same env as before -- the multi-input path is purely additive and the
    contract stays paradigm-symmetric (same keys for imperative classic and
    imperative-over-Connect). It leaks no solution: only locations + neutral names.
    """
    aux = getattr(state, "aux_inputs", None) or {}
    if not aux:
        return {}
    env = {"AGENT_AUX_INPUTS": json.dumps(aux, sort_keys=True)}
    for name, path in aux.items():
        env[f"AGENT_AUX_INPUT_{name.upper()}"] = path
    return env


class AgentBrain(Protocol):
    """Proposes the next turn given the loop state and the arm manifest."""

    name: str

    def propose(self, state: LoopState, arm: Any) -> Proposal:
        ...


class SparkExecutor(Protocol):
    """Runs the structural gate and executes proposals; measures cost."""

    name: str

    def run_gate(self, proposal: Proposal, arm: Any, state: LoopState) -> GateOutcome:
        ...

    def run_execute(self, proposal: Proposal, arm: Any, state: LoopState) -> ExecOutcome:
        ...

    def reachable(self) -> bool:
        """True if this executor can actually run (live: Connect backend up)."""
        ...

    # Real executors (local / Connect) additionally implement `read_table(name)`,
    # returning a Spark DataFrame for a materialized table so the runner can grade
    # the ACTUAL output. The runner detects this capability via `hasattr`; a
    # replay executor that has no real tables simply omits it and the runner uses
    # the canned output_metrics shortcut instead. Signature when present:
    #   def read_table(self, name: str) -> "pyspark.sql.DataFrame": ...
