"""Crash-safety of the LLM-proposal path (the wedge/crash root-cause fix).

brain.propose() is the uncontrolled, network-bound LLM step of the serial sweep.
Before this hardening a single stuck or failing Anthropic call could (a) WEDGE the
run (a stuck socket, inline in the loop, with no harness-owned cancellation) or (b)
CRASH the whole batch (an API exception propagating up through run_episode ->
run_cell -> main, losing all progress). These tests prove the three guarantees:

  1. SSOT client bounds: the client is built timeout=120, max_retries=6.
  2. PER-CELL crash-safety: propose() raising / 429-after-retries / an unexpected
     error is caught, recorded as a structured failed ResultRow with a precise
     exit_class, and the sweep CONTINUES to the next cell. The batch never dies.
  3. HARNESS-OWNED hard cancellation: a propose() that HANGS past the harness
     wall-clock bound is killed (process-group SIGKILL) and bounded, not wedged.

Plus: token/usage accounting is preserved across the killable-subprocess boundary
on the success path.

No network and no anthropic SDK call is made: the killable subprocess worker is
driven through its SSA_PROPOSE_HOOK test seam, so the REAL subprocess path
(including the hang + the os.killpg group kill) is exercised offline.
"""
import json
import os
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner                                        # noqa: E402
from harness.arm_manifest import load_arms                        # noqa: E402
from harness.backends.base import (LoopState, ProposeApiError,    # noqa: E402
                                   ProposeRateLimited, ProposeTimeout)
from harness.backends.live import AnthropicBrain                  # noqa: E402
from harness.schema import EXIT_CLASSES, validate_row             # noqa: E402

ARM = load_arms(os.path.join(STUDY, "arms"))["A"]   # imperative, no gate, max_iter=12

# A task whose input is an upstream table (non-.py) so generate_dataset returns None
# -- the cell needs no real dataset/generator, keeping these tests free of Spark.
TASK_SPEC = {"id": "probe_task", "defects_in_scope": [],
             "input": "upstream.published_table",
             "output_contract": {"table": "agent_out_probe"}}


def _cfg():
    from harness import cost as costmod
    return runner.StudyConfig(
        base_model_id="claude-opus-4-8",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "local", "local"),
    )


class _FakeBrain:
    """A brain whose propose() raises a chosen exception -- stands in for a failing
    Anthropic call without a network. Counts calls so a test can prove the loop
    aborted after exactly one failed propose (no retry storm in-episode)."""

    name = "fake"

    def __init__(self, exc):
        self._exc = exc
        self.calls = 0

    def propose(self, state, arm):
        self.calls += 1
        raise self._exc


def _stub_executor(*_a, **_k):
    """Minimal executor: the propose-aborted episode only ever calls _advance() (a
    getattr-guarded no-op here) and the no-table grading path. No Spark needed."""
    return object()


def _sweep(make_brain, n_seeds=3, backend="live"):
    """Drive _run_cell_safe over several seeds (mimics main()'s inner loop) and return
    the rows. If crash-safety holds, every cell returns a row and none raises."""
    cfg = _cfg()
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        for seed in range(n_seeds):
            row = runner._run_cell_safe(TASK_SPEC, ARM, seed, cfg, make_brain,
                                        _stub_executor, tmp, clock=1750000000.0,
                                        backend=backend)
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# 1. SSOT client bounds
# ---------------------------------------------------------------------------
def test_client_built_with_ssot_bounds():
    """The Anthropic client is constructed from the repo SSOT bounds (`_build_anthropic_client`)
    -- baked into the repo, not a run-local worktree edit. Construction reads ANTHROPIC_API_KEY
    but makes no network call, so a dummy key suffices.

    MERGE RECONCILIATION (#31 propose-hardening x #40 raised-timeout): this stack
    originally pinned the SSOT to timeout=120, max_retries=6, but #40 DELIBERATELY raised
    the per-request timeout (heavy opus-4-8 turns legitimately exceed 120s and were being
    killed + mislabeled as harness_error) to the env-overridable ANTHROPIC_REQUEST_TIMEOUT_S
    (default 300) and dropped max_retries to ANTHROPIC_MAX_RETRIES=2 (so 300x(1+2)=900s stays
    under the 1800s per-cell guard). The SSOT STRUCTURE (one builder for both the in-process
    client and the killable subprocess worker) is preserved; only the VALUES follow #40."""
    from harness.backends import live
    old = os.environ.get("ANTHROPIC_API_KEY")
    os.environ["ANTHROPIC_API_KEY"] = "test-key-not-used"
    try:
        client = live._build_anthropic_client()
    finally:
        if old is None:
            os.environ.pop("ANTHROPIC_API_KEY", None)
        else:
            os.environ["ANTHROPIC_API_KEY"] = old
    assert client.max_retries == live.ANTHROPIC_MAX_RETRIES, client.max_retries
    assert client.timeout == live.ANTHROPIC_REQUEST_TIMEOUT_S, client.timeout
    # the raised default is the whole point of the #40 reconciliation
    assert client.timeout == 300, client.timeout


# ---------------------------------------------------------------------------
# 2(i). propose() raises -> cell recorded as failed + sweep continues
# ---------------------------------------------------------------------------
def test_propose_raise_fails_soft_and_sweep_continues():
    brains = []

    def make_brain(task, arm, seed):
        b = _FakeBrain(ProposeApiError("boom: connection reset after retries"))
        brains.append(b)
        return b

    rows = _sweep(make_brain, n_seeds=3)
    assert len(rows) == 3, "sweep did not continue across all cells"
    for r in rows:
        assert r.exit_class == "PROPOSE_API_ERROR", r.exit_class
        assert r.task_success is False
        assert r.iterations == 1, "expected exactly one failed propose iteration"
        # the per-iteration record preserves WHY it failed (debuggable transcript)
        assert r.per_iteration[0]["propose_error"]["type"] == "ProposeApiError"
        assert not validate_row(json.loads(r.to_json())), "soft-failed row not schema-valid"
    # one propose call per cell, then abort -- no in-episode retry storm
    assert all(b.calls == 1 for b in brains), [b.calls for b in brains]


def test_unexpected_propose_error_classified_harness_exception():
    """A non-Propose exception from propose() (a genuine harness bug, not an API
    failure) still fails soft -- classified HARNESS_EXCEPTION, recorded, sweep lives."""
    def make_brain(task, arm, seed):
        return _FakeBrain(ValueError("unexpected harness bug in propose"))

    rows = _sweep(make_brain, n_seeds=2)
    assert len(rows) == 2
    for r in rows:
        assert r.exit_class == "HARNESS_EXCEPTION", r.exit_class


def test_factory_failure_caught_by_per_cell_net():
    """A failure OUTSIDE the episode loop (here: make_brain itself raising) is the
    per-cell net's job -- run_cell never gets to run_episode, yet the cell is still
    recorded HARNESS_EXCEPTION and the sweep continues."""
    def make_brain(task, arm, seed):
        raise RuntimeError("could not construct brain for this cell")

    rows = _sweep(make_brain, n_seeds=2)
    assert len(rows) == 2
    for r in rows:
        assert r.exit_class == "HARNESS_EXCEPTION", r.exit_class
        assert "could not construct brain" in (r.notes or "")


# ---------------------------------------------------------------------------
# 2(iv). a 429-style exception is classified and fails soft
# ---------------------------------------------------------------------------
def _write_hook(tmp, body):
    p = os.path.join(tmp, "hook.py")
    with open(p, "w") as f:
        f.write(body)
    return p


_HOOK_429 = """
class RateLimitError(Exception):
    status_code = 429
def messages_create(req):
    raise RateLimitError("rate_limit_error: too many requests, slow down")
"""

_HOOK_OK = """
def messages_create(req):
    return {"text": "```python\\nprint('ok')\\n```\\nCOMMAND: python",
            "input_tokens": 11, "output_tokens": 22, "stop_reason": "end_turn"}
"""

_HOOK_HANG = """
import time
def messages_create(req):
    time.sleep(120)
    return {"text": "", "input_tokens": 0, "output_tokens": 0, "stop_reason": None}
"""


def _bounded_brain(hook_path, timeout_s):
    os.environ["SSA_PROPOSE_HOOK"] = hook_path
    return AnthropicBrain("claude-opus-4-8", "task prompt", bounded=True,
                          propose_timeout_s=timeout_s)


def _clear_hook():
    os.environ.pop("SSA_PROPOSE_HOOK", None)


def test_429_classified_and_fails_soft():
    with tempfile.TemporaryDirectory() as tmp:
        hook = _write_hook(tmp, _HOOK_429)
        brain = _bounded_brain(hook, timeout_s=30)
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        try:
            raised = None
            try:
                brain.propose(st, ARM)
            except ProposeRateLimited as e:
                raised = e
        finally:
            _clear_hook()
    assert raised is not None, "a 429 was not surfaced as ProposeRateLimited"
    assert raised.exit_class == "PROPOSE_RATE_LIMIT"
    assert "PROPOSE_RATE_LIMIT" in EXIT_CLASSES

    # and end-to-end through the runner: recorded + sweep continues. Rebuild inside a
    # live tmp so the hook file exists for the subprocess at propose time.
    with tempfile.TemporaryDirectory() as tmp2:
        hook2 = _write_hook(tmp2, _HOOK_429)

        def make_brain2(task, arm, seed):
            return _bounded_brain(hook2, timeout_s=30)

        try:
            rows = _sweep(make_brain2, n_seeds=2)
        finally:
            _clear_hook()
    assert len(rows) == 2
    for r in rows:
        assert r.exit_class == "PROPOSE_RATE_LIMIT", r.exit_class


# ---------------------------------------------------------------------------
# 3(ii). propose() hangs past the bound -> bounded + recorded + sweep continues
# ---------------------------------------------------------------------------
def test_propose_hang_is_bounded_and_recorded():
    with tempfile.TemporaryDirectory() as tmp:
        hook = _write_hook(tmp, _HOOK_HANG)
        brain = _bounded_brain(hook, timeout_s=2)
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        t0 = time.time()
        try:
            err = None
            try:
                brain.propose(st, ARM)
            except ProposeTimeout as e:
                err = e
            elapsed = time.time() - t0
        finally:
            _clear_hook()
        assert err is not None, "a hung propose() was not bounded"
        assert err.exit_class == "PROPOSE_TIMEOUT"
        # bounded near the 2s wall budget, NOT the 120s the hook would otherwise sleep
        assert elapsed < 30, f"propose() not bounded promptly: {elapsed:.1f}s"

    # end-to-end: a hanging cell is recorded PROPOSE_TIMEOUT and the next cell runs
    with tempfile.TemporaryDirectory() as tmp2:
        hook2 = _write_hook(tmp2, _HOOK_HANG)

        def make_brain(task, arm, seed):
            return _bounded_brain(hook2, timeout_s=2)

        try:
            rows = _sweep(make_brain, n_seeds=2)
        finally:
            _clear_hook()
    assert len(rows) == 2, "sweep did not continue past the hung cell"
    for r in rows:
        assert r.exit_class == "PROPOSE_TIMEOUT", r.exit_class
        assert not validate_row(json.loads(r.to_json()))


# ---------------------------------------------------------------------------
# token/usage accounting preserved on the success path (across the subprocess)
# ---------------------------------------------------------------------------
def test_token_accounting_preserved_on_success():
    with tempfile.TemporaryDirectory() as tmp:
        hook = _write_hook(tmp, _HOOK_OK)
        brain = _bounded_brain(hook, timeout_s=30)
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        try:
            p1 = brain.propose(st, ARM)
            st.history.append(p1)
            p2 = brain.propose(st, ARM)
        finally:
            _clear_hook()
    # the parsed proposal survived the JSON round-trip through the killable subprocess
    assert p1.code.strip() and p1.command == "python", (p1.code, p1.command)
    assert p2.stop_reason == "end_turn"
    # usage from BOTH turns accumulated (serialized back from the subprocess each time)
    assert brain.input_tokens == 22, brain.input_tokens   # 11 + 11
    assert brain.output_tokens == 44, brain.output_tokens  # 22 + 22


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
