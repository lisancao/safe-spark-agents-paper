"""Part A.5: per-iteration LLM token usage is PERSISTED into the results row AND the
transcript, so per-cell cost/validity is auditable from the artifacts alone."""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner                                            # noqa: E402
from harness.arm_manifest import load_arms                            # noqa: E402
from harness.backends.base import ExecOutcome, GateOutcome            # noqa: E402
from harness.backends.local import ScriptedBrain                      # noqa: E402


class _TokenBrain(ScriptedBrain):
    """A scripted brain that also reports per-turn token usage (like the live brain)."""
    def __init__(self, per_turn):
        super().__init__([{"code": "x=1\n", "command": "cmd"} for _ in per_turn])
        self._toks = per_turn

    def propose(self, state, arm):
        p = super().propose(state, arm)
        i = min(len(state.history), len(self._toks) - 1)
        p.input_tokens, p.output_tokens = self._toks[i]
        return p


class _OneShotExec:
    name = "stub"

    def reachable(self):
        return True

    def run_gate(self, proposal, arm, state):
        return GateOutcome(failed=False, wall_s=0.1, log="ok")

    def run_execute(self, proposal, arm, state):
        return ExecOutcome(failed=False, completed=True, wall_s=0.1,
                           executor_seconds=0.0, log="Run is COMPLETED")


def test_tokens_persisted_to_row_and_transcript():
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]      # imperative, no gate -> 1 iter to green
    task = {"id": "t", "defects_in_scope": [], "input": "upstream.published_table",
            "output_contract": {}}
    cfg = runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=runner.costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)

    with tempfile.TemporaryDirectory() as tmp:
        row = runner.run_cell(task, arm, 1,
                              cfg, lambda *a: _TokenBrain([(123, 45)]),
                              lambda *a: _OneShotExec(), work_dir=tmp, clock=1750000000.0)
        assert row.exit_class == "completed"
        # (1) totals land on the persisted row...
        assert row.input_tokens == 123 and row.output_tokens == 45
        # ...and survive a JSON round-trip (so results.jsonl carries them).
        d = json.loads(row.to_json())
        assert d["input_tokens"] == 123 and d["output_tokens"] == 45
        # (2) per-iteration + episode totals land in the transcript.
        with open(row.transcript_path) as f:
            tr = json.load(f)
        assert tr["tokens"] == {"input": 123, "output": 45}
        assert tr["per_iteration"][0]["tokens"] == {"input": 123, "output": 45}


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
