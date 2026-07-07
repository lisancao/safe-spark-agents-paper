"""Killable subprocess worker: performs ONE Anthropic messages.create and prints a
JSON result.

Invoked by `AnthropicBrain._propose_bounded` as
`python -m harness.backends.propose_worker`, isolated in its OWN process group
(`start_new_session=True`) so the parent can SIGKILL the whole group on timeout --
the harness-owned hard cancellation that bounds a stuck socket even if the SDK's
own timeout misbehaves.

Protocol (all JSON):
  * stdin   : the messages.create(**req) kwargs (model/max_tokens/system/messages/...)
  * stdout  : on success `{"ok": true, "text", "input_tokens", "output_tokens",
              "stop_reason"}`; on a handled API failure
              `{"ok": false, "error_type", "message", "status_code"}` (still exit 0,
              so the parent can classify 429-vs-other). A NON-zero exit means the
              worker itself broke (or was killed) -- the parent treats that as an
              API error / timeout.

Testability: if SSA_PROPOSE_HOOK names a Python file defining
`messages_create(req) -> dict`, the worker calls THAT instead of the real client.
This exercises the real killable-subprocess path (including the hang and the
process-group kill) WITHOUT a network or the anthropic SDK.
"""
from __future__ import annotations

import json
import os
import sys


def _do_create(req: dict) -> dict:
    hook = os.environ.get("SSA_PROPOSE_HOOK")
    if hook:
        ns: dict = {}
        with open(hook) as f:
            exec(compile(f.read(), hook, "exec"), ns)  # noqa: S102 -- test hook only
        return ns["messages_create"](req)
    # real path: SSOT client (timeout=120, max_retries=6) + one create.
    from harness.backends.live import _messages_create
    return _messages_create(req)


def main() -> int:
    req = json.loads(sys.stdin.read())
    try:
        result = _do_create(req)
        sys.stdout.write(json.dumps({"ok": True, **result}))
        return 0
    except BaseException as e:  # noqa: BLE001 -- report ANY failure in-band, exit 0
        # `status_code` is set by anthropic.APIStatusError subclasses (e.g. 429 on
        # RateLimitError); carried back so the parent classifies the failure precisely.
        sys.stdout.write(json.dumps({
            "ok": False,
            "error_type": type(e).__name__,
            "message": str(e)[:1000],
            "status_code": getattr(e, "status_code", None),
        }))
        return 0


if __name__ == "__main__":
    sys.exit(main())
