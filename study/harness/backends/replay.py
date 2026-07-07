"""Offline replay backend: deterministic, no LLM, no Spark, no network.

Replays a recorded *episode trace* so the entire pipeline -- loop control,
structural gate, cost accounting, blind grading, results.jsonl emission -- runs
end-to-end with zero external dependencies. This is what makes "validate the
instrument offline" possible and is also the substrate for the unit tests.

Episode trace format (JSON):

    {
      "episodes": {
        "<task>|<arm>|<seed>": {
          "iterations": [
            {
              "command": "spark-pipelines dry-run",
              "code_summary": "first draft, references does_not_exist",
              "rationale": "...",
              "gate":  {"failed": true,  "wall_s": 8.1, "error_class": "UNRESOLVED_COLUMN (SQLSTATE 42703)"},
              "exec":  null
            },
            {
              "command": "spark-pipelines run",
              "code_summary": "fixed column, watermark + quarantine added",
              "gate":  {"failed": false, "wall_s": 7.9},
              "exec":  {"failed": false, "completed": true, "wall_s": 42.0,
                         "executor_seconds": 84.0,
                         "output_metrics": {"d8_rows_dropped": 0, "d8_dollars_dropped": 0.0}}
            }
          ]
        }
      }
    }

A `gate`/`exec` block present but unused (e.g. a gate block for an arm whose
manifest has dry_run_gate=false) is simply never read by the runner, because the
runner consults the arm manifest -- not the trace -- to decide whether to gate.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from .base import AgentBrain, ExecOutcome, GateOutcome, LoopState, Proposal, SparkExecutor


def episode_key(task: str, arm: str, seed: int) -> str:
    return f"{task}|{arm}|{seed}"


class _Episode:
    def __init__(self, iters: List[Dict[str, Any]]):
        self.iters = iters
        self.cursor = 0

    def current(self) -> Dict[str, Any]:
        if self.cursor >= len(self.iters):
            raise IndexError("replay episode exhausted: trace has fewer iterations than the loop ran")
        return self.iters[self.cursor]

    def advance(self) -> None:
        self.cursor += 1


class ReplayBackend:
    """Holds a loaded trace and vends a brain + executor bound to one episode."""

    def __init__(self, trace: Dict[str, Any]):
        self.episodes = trace.get("episodes", {})

    @staticmethod
    def from_file(path: str) -> "ReplayBackend":
        with open(path) as f:
            return ReplayBackend(json.load(f))

    def episode(self, task: str, arm: str, seed: int) -> _Episode:
        key = episode_key(task, arm, seed)
        if key not in self.episodes:
            raise KeyError(f"no replay episode for {key!r}; available: {sorted(self.episodes)}")
        return _Episode(list(self.episodes[key]["iterations"]))


class ReplayBrain(AgentBrain):
    name = "replay"

    def __init__(self, episode: _Episode):
        self._ep = episode

    def propose(self, state: LoopState, arm: Any) -> Proposal:
        rec = self._ep.current()
        return Proposal(
            iteration=len(state.history),
            code=rec.get("code_summary", ""),
            command=rec["command"],
            rationale=rec.get("rationale", ""),
            replay_tag=rec.get("replay_tag"),
        )


class ReplayExecutor(SparkExecutor):
    name = "replay"

    def __init__(self, episode: _Episode):
        self._ep = episode

    def reachable(self) -> bool:
        return True

    def run_gate(self, proposal: Proposal, arm: Any, state: LoopState) -> GateOutcome:
        rec = self._ep.current()
        g = rec.get("gate")
        if g is None:
            # No recorded gate for this iteration -> treat as a pass-through gate
            # that found nothing (the proposal was structurally fine).
            return GateOutcome(failed=False, wall_s=8.0, error_class=None,
                               log="replay: no structural finding")
        return GateOutcome(
            failed=bool(g["failed"]),
            wall_s=float(g.get("wall_s", 8.0)),
            error_class=g.get("error_class"),
            log=g.get("log", ""),
        )

    def run_execute(self, proposal: Proposal, arm: Any, state: LoopState) -> ExecOutcome:
        rec = self._ep.current()
        e = rec.get("exec")
        if e is None:
            raise ValueError(
                f"replay trace iteration {self._ep.cursor} has no 'exec' block but the "
                "runner attempted to execute it (gate did not intercept). Trace is "
                "inconsistent with the loop."
            )
        out = ExecOutcome(
            failed=bool(e["failed"]),
            completed=bool(e.get("completed", not e["failed"])),
            wall_s=float(e.get("wall_s", 0.0)),
            executor_seconds=(None if e.get("executor_seconds") is None else float(e["executor_seconds"])),
            error_class=e.get("error_class"),
            log=e.get("log", ""),
            output_metrics=e.get("output_metrics"),
        )
        return out

    def advance(self) -> None:
        self._ep.advance()
