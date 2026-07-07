"""Unified HARNESS-FAULT policy (Part B) — the cross-vendor-approved instrument-safety
mechanism that guarantees a broken instrument can NEVER masquerade as an agent result.

A HARNESS FAULT is an INSTRUMENT failure, never an agent outcome. It is recognized by a
SINGLE notion (`schema.is_harness_fault` over `schema.HARNESS_FAULT_EXIT_CLASSES`) that
unifies BOTH fault paths the policy must cover TOGETHER:

  (i)  the propose-call faults (#31): PROPOSE_TIMEOUT / PROPOSE_API_ERROR /
       PROPOSE_RATE_LIMIT / HARNESS_EXCEPTION (the brain.propose() crash-safety classes), and
  (ii) the new SDP/infra instrument faults (Part A): a missing/relative SDP --spec or an
       absent materialized file, raised as backends.base.HarnessFault (exit_class
       HARNESS_EXCEPTION).

The policy a fault triggers (the (c)->(b)->circuit-breaker design):

  1. RETRY ONCE: wait `HARNESS_FAULT_RETRY_DELAY_S` and re-run the cell exactly one time
     (absorbs transient I/O / daemon flakes).
  2. QUARANTINE + CONTINUE: if the retry also faults, flag the cell `HARNESS_ERROR`
     (exit_class), EXCLUDE it from the H1-H4 statistics, and CONTINUE the sweep. The
     specific underlying fault is preserved in `harness_fault_reason` for the paper's
     excluded-data (quarantine) appendix.
  3. CIRCUIT BREAKER: BEFORE starting the NEXT cell, ABORT THE WHOLE RUN LOUDLY if ANY of
     the three thresholds is breached. The breaker counts QUARANTINED cells (those that
     faulted on BOTH the original attempt and the retry); a cell that recovered on retry is
     a normal result and is not counted. All three limits are NAMED CONSTANTS, easy to tune.
  4. PER-CELL CLEANUP: after ANY harness fault, hard-reset before the next cell so cascades
     do not trip the breaker spuriously (`hard_reset_after_fault`).

The breaker covers both fault paths together: counts accumulate regardless of WHICH path
produced the fault, so a propose-throttle hitting only the imperative batch trips the same
breaker as an SDP path break.
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from harness.schema import is_harness_fault


def _default_log(msg: str) -> None:
    """Default progress logger -- stderr, in the runner's `[runner] ...` style."""
    print(f"[runner] {msg}", file=sys.stderr)


def _cell_ident(row: Any) -> str:
    """A human-readable cell id for logs, from whatever the row carries."""
    rid = getattr(row, "run_id", None)
    if rid:
        return str(rid)
    task, arm, seed = (getattr(row, "task", None), getattr(row, "arm", None),
                       getattr(row, "seed", None))
    if task is not None and arm is not None:
        return f"{task}__{arm}__seed{seed}"
    return "<cell>"

# ---------------------------------------------------------------------------
# Tunable policy constants (NAMED, in ONE place).
# ---------------------------------------------------------------------------
# (1) retry-once: how long to wait before the single retry of a faulted cell.
HARNESS_FAULT_RETRY_DELAY_S = 5.0

# (3) circuit-breaker thresholds. A breach is count > limit (STRICTLY greater), so e.g.
# PER_ARM_HARNESS_FAULT_LIMIT = 1 means the SECOND quarantined fault in one arm trips it.
#   * global: > 3 quarantined faults over the whole sweep (~1.5% of a N=220 run).
GLOBAL_HARNESS_FAULT_LIMIT = 3
#   * per arm: > 1 quarantined fault within ANY single arm (A / A2 / B / B1 / B2).
PER_ARM_HARNESS_FAULT_LIMIT = 1
#   * per task-complexity bin: > 1 quarantined fault within ANY single complexity bin.
PER_BIN_HARNESS_FAULT_LIMIT = 1

# The quarantine exit_class (kept in lockstep with schema.EXIT_CLASSES / .HARNESS_ERROR).
QUARANTINE_EXIT_CLASS = "HARNESS_ERROR"

# ---------------------------------------------------------------------------
# Task-complexity bin (the breaker's per-bin stratum).
# ---------------------------------------------------------------------------
COMPLEXITY_BINS = ("low", "medium", "high")
# The current corpora carry NO explicit complexity field, so the bin is derived from the
# number of in-scope defects (a real, data-present complexity proxy) with NAMED bounds.
# If a future corpus adds an explicit `complexity` / `complexity_bin` / `tier` field, it is
# preferred verbatim (forward-compatible) when it names one of COMPLEXITY_BINS.
_COMPLEXITY_LOW_MAX_DEFECTS = 3        # <= 3 in-scope defects -> "low"
_COMPLEXITY_HIGH_MIN_DEFECTS = 5       # >= 5 in-scope defects -> "high"; else "medium"


def task_complexity_bin(task_spec: Dict[str, Any]) -> str:
    """Deterministic complexity bin for a task spec. Prefers an explicit corpus field;
    else bins by in-scope defect count with the named bounds above."""
    for key in ("complexity_bin", "complexity", "tier"):
        v = task_spec.get(key)
        if isinstance(v, str) and v.lower() in COMPLEXITY_BINS:
            return v.lower()
    n = len(task_spec.get("defects_in_scope") or [])
    if n <= _COMPLEXITY_LOW_MAX_DEFECTS:
        return "low"
    if n >= _COMPLEXITY_HIGH_MIN_DEFECTS:
        return "high"
    return "medium"


# ---------------------------------------------------------------------------
# Loud abort when the breaker trips.
# ---------------------------------------------------------------------------
class CircuitBreakerTripped(Exception):
    """Raised in the sweep (OUTSIDE the per-cell net) to ABORT THE WHOLE RUN LOUDLY when a
    breaker threshold is breached. Deliberately NOT a HarnessFault/ProposeError, so the
    per-cell `_run_cell_safe` net never swallows it -- it propagates to main() and stops
    the sweep, the same deliberate-abort semantics as Ctrl-C / SystemExit."""

    def __init__(self, message: str, counters: Dict[str, Any]):
        super().__init__(message)
        self.counters = counters


@dataclass
class QuarantineRecord:
    """One excluded cell, for the quarantine report (task, seed, arm, exit_class, reason)."""
    task: str
    seed: int
    arm: str
    exit_class: str        # always QUARANTINE_EXIT_CLASS ("HARNESS_ERROR")
    reason: str            # the specific underlying fault class (PROPOSE_TIMEOUT, ...)
    complexity_bin: str


@dataclass
class HarnessFaultTracker:
    """Accumulates QUARANTINED harness faults across the sweep and enforces the breaker."""
    global_count: int = 0
    per_arm: Dict[str, int] = field(default_factory=dict)
    per_bin: Dict[str, int] = field(default_factory=dict)
    records: List[QuarantineRecord] = field(default_factory=list)

    def record_quarantine(self, task: str, seed: int, arm: str, reason: str,
                          complexity_bin: str) -> QuarantineRecord:
        self.global_count += 1
        self.per_arm[arm] = self.per_arm.get(arm, 0) + 1
        self.per_bin[complexity_bin] = self.per_bin.get(complexity_bin, 0) + 1
        rec = QuarantineRecord(task=task, seed=seed, arm=arm,
                               exit_class=QUARANTINE_EXIT_CLASS, reason=reason,
                               complexity_bin=complexity_bin)
        self.records.append(rec)
        return rec

    def breaker_breach(self) -> Optional[str]:
        """Return a human-readable breach reason if ANY threshold is breached, else None.
        Checks ALL three independently so the message names every breached limit."""
        breaches: List[str] = []
        if self.global_count > GLOBAL_HARNESS_FAULT_LIMIT:
            breaches.append(
                f"global harness-fault count {self.global_count} > "
                f"{GLOBAL_HARNESS_FAULT_LIMIT}")
        hot_arms = {a: c for a, c in self.per_arm.items() if c > PER_ARM_HARNESS_FAULT_LIMIT}
        if hot_arms:
            breaches.append(
                f"per-arm harness-fault count > {PER_ARM_HARNESS_FAULT_LIMIT} in {hot_arms}")
        hot_bins = {b: c for b, c in self.per_bin.items() if c > PER_BIN_HARNESS_FAULT_LIMIT}
        if hot_bins:
            breaches.append(
                f"per-complexity-bin harness-fault count > {PER_BIN_HARNESS_FAULT_LIMIT} "
                f"in {hot_bins}")
        return "; ".join(breaches) if breaches else None

    def counters(self) -> Dict[str, Any]:
        return {"global": self.global_count, "per_arm": dict(self.per_arm),
                "per_bin": dict(self.per_bin), "n_quarantined": len(self.records)}

    def check_breaker(self) -> None:
        """ABORT THE WHOLE RUN LOUDLY (raise CircuitBreakerTripped) if any threshold is
        breached. Call this BEFORE starting the next cell."""
        breach = self.breaker_breach()
        if breach is not None:
            raise CircuitBreakerTripped(
                "HARNESS-FAULT CIRCUIT BREAKER TRIPPED — aborting the whole run before the "
                f"next cell: {breach}. Quarantined cells so far: "
                f"{[(r.task, r.seed, r.arm, r.reason) for r in self.records]}. "
                "The instrument is unreliable; FIX IT and re-run rather than trusting a "
                "sweep with this many instrument failures.",
                self.counters())


def quarantine_row(row: Any, reason: str) -> Any:
    """Mutate a soft-failed ResultRow into a QUARANTINED row: exit_class -> HARNESS_ERROR
    (excluded from H1-H4 by analyze.py), with the specific underlying fault class preserved
    in `harness_fault_reason`. The cell NEVER accrues toward max_iterations (its iteration
    accounting is left intact for the audit, but it is no longer an agent outcome)."""
    row.harness_fault_reason = reason
    row.exit_class = QUARANTINE_EXIT_CLASS
    prev = row.notes or ""
    note = f"QUARANTINED (HARNESS_ERROR; underlying {reason}); EXCLUDED from H1-H4 stats"
    row.notes = (f"{note}. {prev}".strip())[:1000]
    return row


# ---------------------------------------------------------------------------
# (1)+(2) per-cell retry-once -> quarantine orchestration (testable in isolation).
# ---------------------------------------------------------------------------
def process_cell(run_fn: Callable[[], Any], *,
                 retry_delay_s: float = HARNESS_FAULT_RETRY_DELAY_S,
                 sleep: Callable[[float], None] = time.sleep,
                 cleanup: Optional[Callable[[], None]] = None,
                 log: Optional[Callable[[str], None]] = None) -> Tuple[Any, Optional[str]]:
    """Run ONE cell under the retry-once -> quarantine-continue policy.

    `run_fn()` runs the cell and returns a (soft-failed-safe) ResultRow; it must NOT raise
    for cell-level errors (that is `_run_cell_safe`'s job). Returns `(row, reason)` where
    `reason` is None when the cell did NOT end quarantined (a clean result OR a transient
    fault that the single retry recovered), or the specific underlying fault class when the
    returned row was quarantined (exit_class now HARNESS_ERROR).

    `cleanup` (the per-cell hard reset) runs after EACH observed fault, so a fault never
    leaks zombie processes / locked ports into the retry or the next cell.

    `log` receives one-line progress messages (default: stderr in the `[runner] ...`
    style). The retry path is NOT silent: a fault and the pending retry are ANNOUNCED
    before the wait, and the retry's resolution (recovered vs quarantined) is announced
    after -- so an operator never sees an unexplained multi-second hang.
    """
    emit = log or _default_log
    row = run_fn()
    if not is_harness_fault(row.exit_class):
        return row, None                       # clean agent result -> nothing to do
    # (1) RETRY ONCE: ANNOUNCE the fault + pending retry, then hard-reset, wait, re-run.
    fault_reason = getattr(row, "harness_fault_reason", None) or row.exit_class
    ident = _cell_ident(row)
    emit(f"HARNESS FAULT on {ident}: {fault_reason} (exit_class={row.exit_class}) — "
         f"hard-reset + retrying ONCE after {retry_delay_s:.0f}s")
    if cleanup is not None:
        cleanup()
    sleep(retry_delay_s)
    row = run_fn()
    if not is_harness_fault(row.exit_class):
        emit(f"HARNESS FAULT on {ident} RECOVERED on retry (exit_class={row.exit_class}); "
             f"not quarantined")
        return row, None                       # transient -> recovered; not counted
    # (2) the retry ALSO faulted -> QUARANTINE this cell. Prefer a SPECIFIC reason the
    # cell already carries (a HarnessFault's SDP_SPEC_MISSING etc.) over the unified
    # exit_class bucket, so the quarantine report names the real instrument failure.
    reason = getattr(row, "harness_fault_reason", None) or row.exit_class
    emit(f"HARNESS FAULT on {ident} persisted on retry: {reason}; "
         f"QUARANTINING (HARNESS_ERROR)")
    if cleanup is not None:
        cleanup()                               # reset before the sweep moves on
    quarantine_row(row, reason)
    return row, reason


# ---------------------------------------------------------------------------
# (4) per-cell hard reset (reuse the process-group teardown technique).
# ---------------------------------------------------------------------------
def _reap_zombie_children() -> int:
    """Reap any defunct (zombie) direct children of this process so a cascade of dead
    Spark/Connect launchers cannot accumulate. The subprocess executors (ConnectExecutor
    `_run`, the killable propose worker, the LocalConnectServer) each start their child in
    its OWN session (`start_new_session=True`) and SIGKILL the whole GROUP on timeout, so
    the leak this guards against is an un-waited child, not a live one. Returns the count
    reaped."""
    reaped = 0
    try:
        while True:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
            reaped += 1
    except ChildProcessError:
        pass            # no children to wait on
    except OSError:
        pass
    return reaped


def hard_reset_after_fault(local_server: Any = None,
                           extra_cleanup: Optional[Callable[[], None]] = None) -> Dict[str, Any]:
    """Hard-reset the local execution substrate after a harness fault, BEFORE the next cell,
    so a transient break does not cascade and trip the breaker spuriously (Part B.4).

      * reap zombie Spark/Connect launcher children (process-group teardown technique);
      * if a long-lived `local_server` is present, leave it UP if it is still reachable
        (subsequent SDP cells dial it); only flag it when it has died, so the operator sees
        the real cause rather than a wall of misattributed cell failures;
      * run any caller-supplied `extra_cleanup` (e.g. freeing a per-cell lock/dir).

    Best-effort and NEVER raises -- a cleanup failure must not itself abort the sweep.
    Returns a small report dict for logging/tests."""
    report: Dict[str, Any] = {"reaped_children": 0, "local_server_alive": None, "notes": []}
    try:
        report["reaped_children"] = _reap_zombie_children()
    except Exception as e:  # noqa: BLE001
        report["notes"].append(f"reap failed: {type(e).__name__}: {e}")
    if local_server is not None:
        try:
            port = getattr(local_server, "port", None)
            checker = getattr(local_server, "_port_open", None)
            alive = bool(checker(port)) if (callable(checker) and port is not None) else None
            report["local_server_alive"] = alive
            if alive is False:
                report["notes"].append(
                    "local Spark Connect server is DOWN after the fault; subsequent SDP "
                    "cells will fault until it is restarted (the breaker will catch a run "
                    "of these).")
        except Exception as e:  # noqa: BLE001
            report["notes"].append(f"server health check failed: {type(e).__name__}: {e}")
    if extra_cleanup is not None:
        try:
            extra_cleanup()
        except Exception as e:  # noqa: BLE001
            report["notes"].append(f"extra_cleanup failed: {type(e).__name__}: {e}")
    return report
