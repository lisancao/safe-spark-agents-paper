"""Compute / cost accounting for H2 (pre-reg §5 "compute-to-correct").

H2 asks whether the structural dry-run gate saves compute relative to
execute-to-debug. To answer it defensibly we need, per iteration, the
*executor-seconds* and *USD* actually consumed, plus the fraction of failing
iterations that the gate intercepted before any executor ran.

Two cost surfaces, instrumented separately (pre-reg §5 "instrument BOTH an
execute-only path (Arm A) and a gated path"):

  * EXECUTE iteration  -> runs on the live cluster. Cost = executor-seconds x
    price. Executor-seconds come from real metrics (Spark REST API
    `/applications/<id>/executors` `totalDuration`, or k8s pod uptime); offline
    we derive them from the recorded execution wall-clock x executor count.
  * DRY-RUN gate iteration -> driver-only structural analysis. NO executors are
    launched, so executor-seconds = 0 and USD = 0. Empirically ~8 s wall
    (pre-reg §5). This is the whole point of H2: a structurally-broken proposal
    is rejected for $0 instead of paying for a doomed cluster run.

`executor_config` (instances, cores, price) is part of every result row so the
$ figure is always traceable to a declared price at a declared scale.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ExecutorConfig:
    """Declared cluster shape + price. Carried verbatim into every result row."""

    instances: int                      # number of executors
    cores_per_executor: int             # vCPU per executor
    memory_gb_per_executor: float
    price_usd_per_executor_hour: float  # the on-demand price used for $ figures
    provider: str = "k8s"               # k8s | emr | databricks | local
    instance_type: str = "unspecified"

    def to_dict(self) -> Dict[str, object]:
        return {
            "instances": self.instances,
            "cores_per_executor": self.cores_per_executor,
            "memory_gb_per_executor": self.memory_gb_per_executor,
            "price_usd_per_executor_hour": self.price_usd_per_executor_hour,
            "provider": self.provider,
            "instance_type": self.instance_type,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ExecutorConfig":
        return ExecutorConfig(
            instances=int(d["instances"]),
            cores_per_executor=int(d["cores_per_executor"]),
            memory_gb_per_executor=float(d["memory_gb_per_executor"]),
            price_usd_per_executor_hour=float(d["price_usd_per_executor_hour"]),
            provider=str(d.get("provider", "k8s")),
            instance_type=str(d.get("instance_type", "unspecified")),
        )


# Kinds of iteration the cost model recognises.
KIND_DRY_RUN = "dry_run"       # structural gate, driver-only, $0
KIND_EXECUTE = "execute"       # ran on the cluster, executors billed


@dataclass
class IterationCost:
    """Cost of a single agent iteration.

    Three compute surfaces are carried side by side so H2 can report effects across
    measurements (D-5): the AUTHORITATIVE `executor_seconds` (the measured stage-diff
    sum when present, else the wall-clock estimate; this is what `usd` is priced on),
    the measured `cpu_seconds` alongside it, and `executor_seconds_wallclock` -- the
    wall_s x instances x busy_fraction derivation kept ALWAYS as an independent
    cross-check (equal to `executor_seconds` only when no live metric was measured).
    """

    kind: str                       # KIND_DRY_RUN | KIND_EXECUTE
    wall_s: float                   # wall-clock for this iteration
    # MEASURED executor-seconds (stage-diff): None when no live metric was obtained,
    # NOT 0.0 -- None is the sentinel for "unmeasured" so a fallback execute or a
    # driver-only gate never masquerades as "measured zero". A driver-only dry-run
    # gate runs NO executors, so it has nothing to measure here -> None.
    executor_seconds: Optional[float]
    usd: float
    failed: bool                    # did the proposal fail at this iteration?
    intercepted_at_dry_run: bool    # failed AND caught by the gate (no executor cost)
    cpu_seconds: Optional[float] = None     # MEASURED CPU-seconds (stage-diff); None if no live metric
    executor_seconds_wallclock: float = 0.0  # wall_s x instances x busy_fraction cross-check (ALWAYS; 0.0 for a gate)
    note: str = ""


def usd_from_executor_seconds(executor_seconds: float, cfg: ExecutorConfig) -> float:
    """Convert executor-seconds to USD at the declared price.

    executor-seconds is already summed across executors, so price is per
    executor-hour: usd = executor_seconds / 3600 * price_per_executor_hour.
    """
    return (executor_seconds / 3600.0) * cfg.price_usd_per_executor_hour


def execute_iteration_cost(
    wall_s: float,
    cfg: ExecutorConfig,
    failed: bool,
    measured_executor_seconds: Optional[float] = None,
    measured_cpu_seconds: Optional[float] = None,
    busy_fraction: float = 1.0,
    note: str = "",
) -> IterationCost:
    """Cost of an EXECUTE iteration (ran on the cluster).

    If `measured_executor_seconds` is provided (from the Spark REST stage-diff, D-5)
    it is AUTHORITATIVE and prices `usd`. Otherwise we derive a defensible upper
    bound from the wall-clock: executor_seconds = wall_s * instances * busy_fraction.
    The full-occupancy default (busy_fraction=1.0) is the conservative, pre-declared
    estimator used when live metrics are unavailable.

    `executor_seconds` carries the MEASURED value (None when no live metric), so a
    fallback iteration is reported as unmeasured rather than as a misleading number;
    `executor_seconds_wallclock` records the wall-clock derivation ALWAYS (even when a
    live metric is present) as an independent cross-check and the $-pricing fallback;
    and `measured_cpu_seconds` is carried alongside -- so a result row reports all
    three compute measurements (D-5).
    """
    wallclock_exec_s = wall_s * cfg.instances * busy_fraction
    # $ is priced on the measured value WHEN PRESENT, else on the wall-clock estimate.
    priced_exec_s = float(measured_executor_seconds) if measured_executor_seconds is not None else wallclock_exec_s
    return IterationCost(
        kind=KIND_EXECUTE,
        wall_s=float(wall_s),
        executor_seconds=(None if measured_executor_seconds is None else float(measured_executor_seconds)),
        usd=usd_from_executor_seconds(priced_exec_s, cfg),
        failed=bool(failed),
        intercepted_at_dry_run=False,
        cpu_seconds=(None if measured_cpu_seconds is None else float(measured_cpu_seconds)),
        executor_seconds_wallclock=wallclock_exec_s,
        note=note or ("derived from wall_s (no live metrics)" if measured_executor_seconds is None else "measured"),
    )


def dry_run_iteration_cost(wall_s: float, failed: bool, note: str = "") -> IterationCost:
    """Cost of a DRY-RUN gate iteration: driver-only, zero executor-seconds, $0.

    `failed` True means the gate rejected the proposal structurally -- which is
    exactly the compute we avoided spending on a doomed cluster run.
    """
    return IterationCost(
        kind=KIND_DRY_RUN,
        wall_s=float(wall_s),
        # driver-only: NO executors ran, so there is nothing to MEASURE here. Use the
        # None "unmeasured" sentinel (NOT 0.0) so a gated run whose execute compute
        # falls back to (None, None) does not aggregate to a misleading measured 0.0.
        executor_seconds=None,
        usd=0.0,
        failed=bool(failed),
        intercepted_at_dry_run=bool(failed),
        cpu_seconds=None,                 # driver-only: no executor CPU to measure
        executor_seconds_wallclock=0.0,   # cross-check: driver-only -> legitimately 0.0
        note=note or "driver-only structural gate; no executors launched",
    )


def no_code_iteration_cost(wall_s: float = 0.0, note: str = "") -> IterationCost:
    """Cost of an iteration whose agent turn produced NO runnable code (no fenced
    code block parsed). Nothing was materialized or run, so there is no compute and
    no $ -- but it IS a FAILED iteration (the agent must retry), and it is NOT a
    dry-run gate intercept (the gate never saw a file). Counted as a zero-cost
    EXECUTE so `failing_iterations` includes it while `dry_run_intercepts` does not.
    """
    return IterationCost(
        kind=KIND_EXECUTE,
        wall_s=float(wall_s),
        executor_seconds=None,            # nothing ran -> unmeasured (not 0.0)
        usd=0.0,
        failed=True,
        intercepted_at_dry_run=False,     # NOT a gate intercept (no file to analyze)
        cpu_seconds=None,
        executor_seconds_wallclock=0.0,   # nothing ran -> legitimately 0.0
        note=note or "no code produced; iteration failed before gate/execute",
    )


def timeout_iteration_cost(wall_s: float = 0.0, note: str = "") -> IterationCost:
    """Cost of an iteration HARD-KILLED at the execution timeout (EXECUTION_TIMEOUT --
    LocalSparkExecutor watchdog or ConnectExecutor process-group kill).

    The kill consumed no ATTRIBUTABLE task compute, so it must NOT be priced via the
    wall-clock fallback (which would charge ~`timeout` seconds of fake executor-seconds
    and non-zero $ for a doomed run). Like `no_code_iteration_cost`, it is a FAILED
    iteration that is NOT a dry-run gate intercept, with $0, `executor_seconds=None`,
    and `executor_seconds_wallclock=0.0`. `wall_s` is carried as the honest elapsed
    time (it does not feed compute/$), so `total_wall_s` stays truthful.
    """
    return IterationCost(
        kind=KIND_EXECUTE,
        wall_s=float(wall_s),
        executor_seconds=None,            # hard-killed: nothing attributable to measure
        usd=0.0,
        failed=True,
        intercepted_at_dry_run=False,     # a timeout kill is NOT a structural gate intercept
        cpu_seconds=None,
        executor_seconds_wallclock=0.0,   # no compute ran -> the cross-check is legitimately 0.0
        note=note or "execution hard-killed at the timeout; no attributable compute",
    )


@dataclass
class RunCost:
    """Aggregated cost over a whole (task, arm, seed) run."""

    total_wall_s: float
    total_executor_seconds: Optional[float]        # MEASURED stage-diff sum; None if never measured
    total_usd: float
    executor_seconds_to_correct: Optional[float]   # compute-to-correct (H2 primary)
    wall_s_to_green: Optional[float]
    failing_iterations: int
    dry_run_intercepts: int
    intercept_fraction: Optional[float]            # dry_run_intercepts / failing_iterations
    reached_correct: bool = True                   # False -> compute-to-correct is the ITT imputation
    # --- D-5: report BOTH measured metrics PLUS the wall-clock cross-check --------
    total_cpu_seconds: Optional[float] = None              # measured CPU-seconds (None if never measured)
    cpu_seconds_to_correct: Optional[float] = None         # measured CPU-seconds up to green
    total_executor_seconds_wallclock: float = 0.0          # wall x slots cross-check (always derivable)
    executor_seconds_wallclock_to_correct: Optional[float] = None  # cross-check up to green

    def to_dict(self) -> Dict[str, object]:
        return {
            "total_wall_s": self.total_wall_s,
            "total_executor_seconds": self.total_executor_seconds,
            "total_usd": self.total_usd,
            "executor_seconds_to_correct": self.executor_seconds_to_correct,
            "wall_s_to_green": self.wall_s_to_green,
            "failing_iterations": self.failing_iterations,
            "dry_run_intercepts": self.dry_run_intercepts,
            "intercept_fraction": self.intercept_fraction,
            "reached_correct": self.reached_correct,
            "total_cpu_seconds": self.total_cpu_seconds,
            "cpu_seconds_to_correct": self.cpu_seconds_to_correct,
            "total_executor_seconds_wallclock": self.total_executor_seconds_wallclock,
            "executor_seconds_wallclock_to_correct": self.executor_seconds_wallclock_to_correct,
        }


def aggregate(iters: List[IterationCost], green_iter_index: Optional[int],
              completed: bool = True) -> RunCost:
    """Aggregate per-iteration costs into a RunCost.

    `green_iter_index` is the 0-based index of the iteration that first produced
    a correct output (None if the run never went green). For a GREEN run,
    compute-to-correct sums executor-seconds up to and including that iteration --
    the compute actually spent to reach the first correct result.

    B9 (H2 success bias): a run that never reached a correct output still consumed
    compute. We do NOT silently set its compute-to-correct to null and drop it
    from H2 (that would condition the estimate on success and flatter whichever
    arm fails more). Instead, for a non-green run compute-to-correct =
    `total_executor_seconds` (all compute it burned without succeeding) and we tag
    it `reached_correct=False`. analyze.py then runs BOTH an intention-to-treat
    H2 over all cells (this imputed value, a conservative lower bound on the true
    cost-to-correct) AND a complete-case sensitivity over both-green cells, and
    reports both. The rule + sensitivity are pre-declared in DEVIATIONS.md (B9).

    D-5: `total_executor_seconds` / `total_cpu_seconds` (and the *_to_correct slices)
    are MEASURED sums and are None when NO execute iteration obtained a live metric
    (a driver-only gate and a fallback execute both contribute None) -- so the ITT
    imputation above is None for a fully-unmeasured non-green run. The always-present
    wall-clock cross-check (`total_executor_seconds_wallclock`) still carries the
    estimate in that case.
    """
    total_wall = sum(i.wall_s for i in iters)
    total_usd = sum(i.usd for i in iters)
    total_wallclock = sum(i.executor_seconds_wallclock for i in iters)
    failing = sum(1 for i in iters if i.failed)
    intercepts = sum(1 for i in iters if i.intercepted_at_dry_run)

    # MEASURED surfaces (executor_seconds, cpu_seconds) are reported only when at
    # least one iteration actually measured them (None otherwise) -- a driver-only
    # gate and a fallback execute both contribute None, so a gated run with no live
    # metric aggregates to None, NOT a misleading 0.0. The wall-clock cross-check is
    # summed unconditionally (it is always present, legitimately 0.0 for a gate).
    def _sum_measured(items: List[IterationCost], attr: str) -> Optional[float]:
        vals = [v for v in (getattr(i, attr) for i in items) if v is not None]
        return sum(vals) if vals else None

    total_exec = _sum_measured(iters, "executor_seconds")
    total_cpu = _sum_measured(iters, "cpu_seconds")

    if green_iter_index is not None:
        upto = iters[: green_iter_index + 1]
        exec_to_correct: Optional[float] = _sum_measured(upto, "executor_seconds")
        wall_to_green: Optional[float] = sum(i.wall_s for i in upto)
        cpu_to_correct: Optional[float] = _sum_measured(upto, "cpu_seconds")
        wallclock_to_correct: Optional[float] = sum(i.executor_seconds_wallclock for i in upto)
        reached_correct = True
    elif not completed:
        # ITT imputation: charge all compute spent; flag as not-yet-correct.
        exec_to_correct = total_exec
        wall_to_green = total_wall
        cpu_to_correct = total_cpu
        wallclock_to_correct = total_wallclock
        reached_correct = False
    else:
        exec_to_correct = None
        wall_to_green = None
        cpu_to_correct = None
        wallclock_to_correct = None
        reached_correct = False

    intercept_fraction = (intercepts / failing) if failing > 0 else None
    return RunCost(
        total_wall_s=total_wall,
        total_executor_seconds=total_exec,
        total_usd=total_usd,
        executor_seconds_to_correct=exec_to_correct,
        wall_s_to_green=wall_to_green,
        failing_iterations=failing,
        dry_run_intercepts=intercepts,
        intercept_fraction=intercept_fraction,
        reached_correct=reached_correct,
        total_cpu_seconds=total_cpu,
        cpu_seconds_to_correct=cpu_to_correct,
        total_executor_seconds_wallclock=total_wallclock,
        executor_seconds_wallclock_to_correct=wallclock_to_correct,
    )
