"""Regression guard: the Anthropic per-request HTTP timeout is env-overridable and
defaults to 300s (no network, no Spark).

`ANTHROPIC_REQUEST_TIMEOUT_S` (live.py) bounds every Messages API call. It was a
hardcoded 120.0 -- too low for heavy opus-4-8 turns (up to 16k max_tokens + extended
thinking), which legitimately take ~50s for 4096 tokens and routinely approach/exceed
120s. At 120s those slow-but-successful turns were killed mid-flight and (after
TRANSIENT_API_FAILURE_LIMIT consecutive hits) mislabeled as harness_error rows. The
constant is now read from the environment with a raised default of 300. The read
happens at import time, so we reload the module under a controlled environment.
"""
import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness.backends import live  # noqa: E402

_ENV = "ANTHROPIC_REQUEST_TIMEOUT_S"


def _timeout_with(env_value):
    """Reload live.py with the env var set to env_value (None == unset) and return
    the resulting ANTHROPIC_REQUEST_TIMEOUT_S; restores the ambient env afterward.

    importlib.reload mutates the module IN PLACE, so we read the scalar out before
    the restore reload clobbers it -- returning the module would hand back a value
    overwritten by the restore."""
    saved = os.environ.get(_ENV)
    try:
        if env_value is None:
            os.environ.pop(_ENV, None)
        else:
            os.environ[_ENV] = env_value
        importlib.reload(live)
        return live.ANTHROPIC_REQUEST_TIMEOUT_S
    finally:
        if saved is None:
            os.environ.pop(_ENV, None)
        else:
            os.environ[_ENV] = saved
        importlib.reload(live)  # restore module to ambient-env state for other tests


def test_default_timeout_is_300_when_env_unset():
    timeout = _timeout_with(None)
    assert timeout == 300.0
    assert isinstance(timeout, float)


def test_env_overrides_timeout():
    assert _timeout_with("450") == 450.0


if __name__ == "__main__":
    test_default_timeout_is_300_when_env_unset()
    test_env_overrides_timeout()
    print("test_live_timeout: 2/2 passed")
