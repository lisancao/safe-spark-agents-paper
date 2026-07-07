"""A-priori task-complexity rubric (corpus v3 §2).

ONE source of truth for the 8-axis complexity rubric used to score every task in
`TASKS.lock.json`. Each task stores `complexity_score`, `complexity_bin`, and the
per-axis `complexity_axes` breakdown; `tests/test_complexity.py` recomputes the
score from the axes through THIS module so the stored number can never drift from
the rubric, exactly as the semantic oracles are single-sourced from quantify.py.

`complexity_score` is a continuous, pre-registered, a-priori task-level covariate
for hypothesis H4 (`silent_defect ~ paradigm * complexity_score + (1|task_id)`);
the Low/Med/High bin is for presentation only.

Complexity is defined as the NUMBER and INTERDEPENDENCE of required-correct
behaviours (DAG depth, state, joins, idempotency, cross-stage invariants), NEVER
non-determinism: every stage keeps a deterministic contract + oracle-checked
invariant. The two highest-weight axes (A6 idempotency, A7 cross-stage
invariants) are the levers used to elevate a medium task into the High bin.
"""
from __future__ import annotations

from typing import Dict

# axis_key -> (weight, "0 -> 3 meaning")
AXES: Dict[str, tuple] = {
    "A1": (2, "DAG depth (interdependent stages): 1 stage -> 4+ stages"),
    "A2": (3, "State management: stateless -> 3+ stateful ops / complex state"),
    "A3": (2, "Joins & aggregations: 0-1 simple -> multi-way / temporal"),
    "A4": (1, "Sinks & fan-out: 1 sink -> dynamic / complex routing"),
    "A5": (2, "Schema handling: fixed -> quarantine / DLQ required"),
    "A6": (3, "Idempotency: append-only -> idempotent across stages"),
    "A7": (3, "Cross-stage invariants: none -> as-of / no-overlap / reconciliation"),
    "A8": (1, "Custom logic (UDF): none -> stateful / complex UDF"),
}

AXIS_KEYS = tuple(AXES.keys())
WEIGHTS = {k: w for k, (w, _) in AXES.items()}

# bins on the weighted sum (a-priori thresholds)
LOW_MAX = 15        # Low  0 .. 15
MED_MAX = 30        # Med 16 .. 30  ;  High 31+


def score(axes: Dict[str, int]) -> int:
    """Weighted sum of the 8 axis values (each 0-3). Raises on a malformed block."""
    missing = set(AXIS_KEYS) - set(axes)
    if missing:
        raise ValueError(f"complexity axes missing {sorted(missing)}")
    extra = set(axes) - set(AXIS_KEYS)
    if extra:
        raise ValueError(f"complexity axes has unknown keys {sorted(extra)}")
    total = 0
    for k in AXIS_KEYS:
        v = axes[k]
        if not isinstance(v, int) or not (0 <= v <= 3):
            raise ValueError(f"axis {k} value {v!r} not an int in 0..3")
        total += WEIGHTS[k] * v
    return total


def bin_of(s: int) -> str:
    if s <= LOW_MAX:
        return "Low"
    if s <= MED_MAX:
        return "Med"
    return "High"


def max_score() -> int:
    return sum(w * 3 for w in WEIGHTS.values())
