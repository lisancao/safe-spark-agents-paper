"""Regression guard: the live brain sends the RIGHT request shape (no network).

The study base model is `claude-opus-4-8`, which REJECTS `temperature`/`top_p`/
`top_k`/`budget_tokens` with a hard 400 and uses ADAPTIVE thinking. So for every
arm the live `AnthropicBrain.build_request` must transmit NO sampling knob and set
`thinking={'type':'adaptive'}`. (The arms still RECORD temperature/top_p as
controlled-variable provenance via `sampling_kwargs`, validated identical across
arms -- we just stop sending them.) For the legacy Claude 4.x family (e.g.
claude-sonnet-4-6) the brain instead sends AT MOST ONE of {temperature, top_p}
(both-specified is a hard 400). This test builds the EXACT kwargs the brain would
pass to `client.messages.create()` -- WITHOUT a key or a network call -- and
asserts both invariants.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import arm_manifest as AM             # noqa: E402
from harness.arm_manifest import load_arms          # noqa: E402
from harness.backends.live import AnthropicBrain, _is_adaptive_thinking_model  # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
SAMPLING_KEYS = {"temperature", "top_p"}
# params claude-opus-4-x rejects with a hard 400 -- none may appear in the request.
OPUS_FORBIDDEN = {"temperature", "top_p", "top_k", "budget_tokens"}


def _request_for(arm):
    brain = AnthropicBrain(arm.base_model_id, "prompt", sampling=AM.sampling_kwargs(arm))
    # build_request does NOT touch the network or need a key
    return brain.build_request(system="sys", messages=[{"role": "user", "content": "x"}])


def test_request_sends_at_most_one_sampling_param_every_arm():
    # invariant holds for BOTH families: opus sends zero, legacy sends one.
    for arm_id, arm in ARMS.items():
        req = _request_for(arm)
        present = SAMPLING_KEYS & set(req)
        assert len(present) <= 1, f"arm {arm_id}: request sends both sampling params: {present}"


def test_opus_request_sends_no_sampling_params_and_uses_adaptive_thinking():
    """The study model claude-opus-4-8 rejects temperature/top_p/top_k/budget_tokens
    (400) and uses adaptive thinking: the request must carry NONE of them and set
    thinking=adaptive, for every arm."""
    for arm_id, arm in ARMS.items():
        assert _is_adaptive_thinking_model(arm.base_model_id), \
            f"arm {arm_id}: expected an opus-4-x base model, got {arm.base_model_id!r}"
        req = _request_for(arm)
        leaked = OPUS_FORBIDDEN & set(req)
        assert not leaked, f"arm {arm_id}: opus request must not send {leaked}"
        assert req.get("thinking") == {"type": "adaptive"}, \
            f"arm {arm_id}: opus request must use adaptive thinking, got {req.get('thinking')!r}"
        # max_tokens is still carried (non-streaming budget).
        assert req.get("max_tokens"), f"arm {arm_id}: lost max_tokens"


def test_legacy_family_still_sends_temperature_only():
    """Guard the legacy claude-sonnet-4-6 path is unchanged: temperature-only, no
    top_p (both-specified is a 400). Uses a synthetic brain, not the live arms."""
    brain = AnthropicBrain("claude-sonnet-4-6", "prompt", sampling={"temperature": 0.0})
    req = brain.build_request(system="sys", messages=[{"role": "user", "content": "x"}])
    assert "temperature" in req and "top_p" not in req, \
        f"legacy path expected temperature-only, got {SAMPLING_KEYS & set(req)}"
    assert "thinking" not in req, "legacy path must not set adaptive thinking"


def test_sampling_value_identical_across_arms():
    sent = {aid: AM.sampling_kwargs(a) for aid, a in ARMS.items()}
    distinct = {tuple(sorted(v.items())) for v in sent.values()}
    assert len(distinct) == 1, f"sampling differs across arms (confound): {sent}"


def test_sampling_control_invariants():
    # the knob we send must be a validated identical-across-arms field
    assert set(AM.SAMPLING_SENT) <= set(AM.SHARED_FIELDS)
    assert set(AM.SAMPLING_SENT) <= set(AM.SAMPLING_CONTROLLED)
    # both controlled params are still validated identical-across-arms (sampling
    # check not silently dropped)
    assert {"temperature", "top_p"} <= set(AM.SHARED_FIELDS)
    # and assert_identical_except_loop still passes for the real arms
    AM.assert_identical_except_loop(ARMS)


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback; failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
