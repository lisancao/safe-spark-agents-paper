"""Live backend: anthropic-driven agent brain + Spark Connect executor.

This is the REAL loop the sweep will use once the Connect backend (catalog swap,
finalized in parallel) is up and an API key is present. It is written to run, but
it is import-safe everywhere: `anthropic` and any network/Spark access are lazily
imported inside methods, so `import live` never fails in an offline environment.

How arms stay identical-except-loop here (pre-reg §3):
  * The base model id, the task prompt text, the sampling params (temperature/
    top_p), and the per-seed dataset are passed in by the RUNNER from the shared
    config -- never chosen by this backend. The brain only varies what the arm
    manifest's LOOP fields say: the paradigm (sdp vs imperative) framing, whether
    the safety skill is linked, the linked skill packs, and the allowed commands.
  * Model stochasticity is a known threat (pre-reg §9): there is no sampling-seed
    knob in the Anthropic API, so reproducibility rests on temperature=0 + a
    fixed prompt + byte-identical per-seed data, with residual nondeterminism
    absorbed by multiple seeds and the random-effects model. The DATA seed is
    fully controlled; that is what `seed` means in a result row.

Skill linking (omnigent handoff): the live brain reads `OMNIGENT_SKILLS` (set by
deploy/spark-omnigent/entrypoint.sh -- the `exec omnigents run --skills ...`
handoff at entrypoint.sh:85) and, for arms whose manifest lists those skills,
loads their SKILL.md text into the system prompt. This is the concrete wiring for
the "partial skill-linking + TODO harness handoff" noted in the deploy README.
When OMNIGENT_SKILLS is unset (the usual case here) the brain falls back to the
in-repo packs under `experiments/safe_agent_study/skills/` (`_DEFAULT_SKILLS_DIR`,
resolved relative to this module), so the SDP arms always get real SKILL.md
guidance instead of hallucinating Databricks `dlt`.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from typing import Any, List, Optional

from .base import (CONNECT_CMD_TIMEOUT_S, PROPOSE_WALL_TIMEOUT_S, AgentBrain, ExecOutcome,
                   GateOutcome, HarnessFault, LoopState, Proposal, ProposeApiError,
                   ProposeRateLimited, ProposeTimeout, SparkExecutor,
                   aux_input_env, aux_locations_text)

# SSOT for the Anthropic client's OWN bounds (deliverable 1). A per-attempt HTTP
# timeout caps each request; max_retries lets the SDK ride out transient 429/5xx/socket
# resets with backoff. Both the in-process client (`_client_lazy`) and the killable
# subprocess worker build the client through `_build_anthropic_client`, so the bounds are
# applied in exactly ONE place. The harness's own wall-clock bound (PROPOSE_WALL_TIMEOUT_S)
# sits ABOVE this retry budget and is the backstop for when the SDK timeout misbehaves.
#
# The actual timeout/retry VALUES are the env-overridable ones defined below
# (ANTHROPIC_REQUEST_TIMEOUT_S, default RAISED 120->300; ANTHROPIC_MAX_RETRIES=2) -- read
# at construction time so the raised timeout applies to BOTH the in-process and the
# subprocess-worker clients (see the rationale comment on ANTHROPIC_REQUEST_TIMEOUT_S).


def _build_anthropic_client():
    """Construct the Anthropic client with the harness's bounds (SSOT).

    Uses the env-overridable ANTHROPIC_REQUEST_TIMEOUT_S (default 300) per attempt and
    ANTHROPIC_MAX_RETRIES retries: a stuck socket is abandoned after the timeout and
    retried with backoff, so an ordinary transient blip self-heals WITHOUT failing the
    cell. `anthropic` is imported lazily so this module stays import-safe in offline
    environments / tests."""
    import anthropic  # lazy: only needed for a live run
    return anthropic.Anthropic(
        timeout=ANTHROPIC_REQUEST_TIMEOUT_S,
        max_retries=ANTHROPIC_MAX_RETRIES)


def _result_from_response(resp) -> dict:
    """Project an Anthropic Messages response into the small JSON-serializable dict
    the brain needs: concatenated text, token usage, stop_reason. Shared by the
    in-process path and the subprocess worker so both serialize usage identically
    (token/usage accounting is preserved across the process boundary)."""
    text = "".join(getattr(b, "text", "") for b in resp.content
                   if getattr(b, "type", "") == "text")
    usage = getattr(resp, "usage", None)
    return {
        "text": text,
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
        "stop_reason": getattr(resp, "stop_reason", None),
    }


def _messages_create(req: dict) -> dict:
    """Build the SSOT client, run ONE STREAMING messages request, return the projected
    result dict. This is the unit of work executed INSIDE the killable subprocess
    (harness/backends/propose_worker.py) -- keeping the SDK config AND the blocking
    network call off the parent's thread, where a stuck socket could otherwise wedge
    the serial sweep even past the SDK's own timeout.

    STREAMING (not the plain non-streaming create): max_tokens was raised to 32000 for
    the opus ADAPTIVE-thinking path. Thinking shares the max_tokens budget, and at 16000
    it routinely exhausted the budget BEFORE the fenced code module emitted -> empty
    `proposal.code` -> a no-code iteration that burns the whole per-cell budget (acute on
    the no-safety arm, whose deliberation runs longer without the skill's idioms;
    DEVIATIONS D-7). 32000 exceeds the SDK's ~16K non-streaming long-request guard, which
    raises ValueError on a non-streaming create(); the streaming path makes the client
    `timeout` per-CHUNK rather than total, so a ~390s/32k generation never trips the 300s
    request timeout, and the whole call still sits well under PROPOSE_WALL_TIMEOUT_S=1200s.
    `get_final_message()` returns the same Message shape as create(), so the projection
    and token accounting are unchanged."""
    client = _build_anthropic_client()
    with client.messages.stream(**req) as stream:
        resp = stream.get_final_message()
    return _result_from_response(resp)


def _worker_pythonpath(existing: Optional[str]) -> str:
    """PYTHONPATH for the propose-worker subprocess: the dir that CONTAINS the
    `harness` package (so `python -m harness.backends.propose_worker` imports), then
    whatever PYTHONPATH the parent had. live.py is harness/backends/live.py, so three
    levels up is the study dir that holds `harness/`. Resolved relative to this module
    so it works from any checkout / worktree -- never a hardcoded path."""
    study_dir = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    return study_dir + (os.pathsep + existing if existing else "")


def _classify_worker_error(result: dict) -> ProposeApiError:
    """Map a worker-reported API failure into the right ProposeError subtype so the
    runner stamps a precise exit_class. A 429 / rate-limit (by HTTP status or by
    SDK error-class name) is PROPOSE_RATE_LIMIT; everything else is PROPOSE_API_ERROR.
    Both already survived the client's `max_retries` retries inside the worker."""
    etype = str(result.get("error_type", ""))
    status = result.get("status_code")
    msg = str(result.get("message", ""))[:500]
    detail = f"{etype}: {msg}" if msg else etype or "unknown API error"
    if status == 429 or "RateLimit" in etype or "rate_limit" in msg.lower():
        return ProposeRateLimited(f"rate-limited after retries ({detail})")
    return ProposeApiError(f"Anthropic API error after retries ({detail})")

# Spark UI keeps only the most recent `spark.ui.retainedStages` (default 1000) stages;
# warn when a before-snapshot nears this so the stage-diff cannot silently undercount
# from mid-run eviction on a misconfigured cluster (study cells create few stages).
_STAGE_RETENTION_WARN_AT = 900

# Default location of the in-repo skill packs (SKILL.md files), resolved RELATIVE
# to this module so it works from any checkout and in any worktree -- never a
# hardcoded absolute path. live.py lives at
# experiments/safe_agent_study/harness/backends/live.py, so the study dir is two
# levels up and the skills live under it. When OMNIGENT_SKILLS is unset (the usual
# case here), the brain falls back to this so `_load_skill('pyspark-sdp')` finds
# the file and `_system_prompt()` injects `=== LINKED SKILL: pyspark-sdp ===`.
_DEFAULT_SKILLS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "skills"))

# HARD per-request timeout for the Anthropic Messages API call. WITHOUT this the SDK's
# underlying httpx read has no deadline, so a stalled response leaves the agent's
# propose() blocked on a socket read FOREVER -- the live calibration wedge (the loop
# advanced to the next iteration's LLM call after materializing output, then the HTTP
# read never returned). With a bounded timeout + a couple of retries, a stalled call
# fails fast and the iteration degrades to a graceful no-code/api_error turn instead of
# hanging the whole sweep. Total bounded worst case per turn ~= timeout x (1 + retries).
#
# Env-overridable; default RAISED from 120 to 300. Heavy opus-4-8 calls (up to 16k
# max_tokens + extended thinking, see AnthropicBrain's max_tokens=16000 default below)
# legitimately exceed 120s:
# direct timed probes against the same model on this gateway show ~1.0s for an 8-token
# reply but ~50s for a 4096-token generation, so a 16k+thinking generation routinely
# approaches or passes 120s. At 120s those slow-but-successful turns were being killed
# mid-flight and (after TRANSIENT_API_FAILURE_LIMIT consecutive hits) mislabeled as
# harness_error rows -- ~27% in the last full sweep, concentrated on heavy/complex cells
# and late large-context iterations, NOT an API outage. 300 x (1 + ANTHROPIC_MAX_RETRIES=2)
# = 900s worst-case per turn still sits safely under the 1800s per-cell wall-clock guard.
ANTHROPIC_REQUEST_TIMEOUT_S = float(os.getenv("ANTHROPIC_REQUEST_TIMEOUT_S", "300"))
ANTHROPIC_MAX_RETRIES = 2
# How many CONSECUTIVE transient API failures (timeout/connection, after the SDK's own
# retries) within ONE cell before we stop pretending it's an agent no-code turn and
# surface it as an infra/model failure. A blip (1-2) degrades to a retried no-code turn;
# a persistent outage escalates so the cell is a harness_error row, not a max_iterations
# attributed to the arm (and so a dead network can't burn the whole per-iteration budget
# x max_iterations before the per-cell guard notices).
TRANSIENT_API_FAILURE_LIMIT = 3


class AgentApiError(RuntimeError):
    """A NON-transient (or persistent) Anthropic API/client failure: auth, quota,
    bad-request, an SDK/validation bug, or repeated transient failures. This is an
    INFRA/MODEL failure, NOT an agent outcome -- raising it (instead of returning an
    empty proposal) lets the runner record the cell as a bounded harness_error row that
    calibration can tell apart from 'the arm produced a bad/incomplete pipeline'."""


def _is_transient_api_error(exc: BaseException) -> bool:
    """True only for EPHEMERAL network/timeout failures worth retrying as a no-code turn
    (the SDK already retried 429/5xx internally). Auth/quota/bad-request/validation are
    NOT transient -- they must surface as an infra/model failure."""
    transient: List[type] = [TimeoutError, ConnectionError]
    try:
        import anthropic
        for name in ("APITimeoutError", "APIConnectionError"):
            t = getattr(anthropic, name, None)
            if isinstance(t, type):
                transient.append(t)
    except Exception:  # noqa: BLE001 -- SDK absent: fall back to the stdlib classes
        pass
    return isinstance(exc, tuple(transient))


def _is_adaptive_thinking_model(model_id: str) -> bool:
    """True for the claude-opus-4-x family, which uses ADAPTIVE thinking and REJECTS
    `temperature`/`top_p`/`top_k`/`budget_tokens` with a hard HTTP 400. For these
    models the request must transmit NO sampling knob and set
    `thinking={'type':'adaptive'}` instead. Matches e.g. 'claude-opus-4-8' and
    'claude-opus-4-8[1m]'. Other models (e.g. claude-sonnet-4-6) keep the
    one-knob temperature path."""
    mid = (model_id or "").lower()
    return mid.startswith("claude-opus-4") or "opus-4" in mid


# --- structural error-class extraction (shared with the E3 battery) ---------
def extract_error_class(log: str) -> Optional[str]:
    """First [ERROR_CLASS] + SQLSTATE from a Spark analysis/execution log."""
    import re
    ec = None
    m = re.search(r"\[([A-Z][A-Z0-9_.]+)\]", log or "")
    if m:
        ec = m.group(1)
    s = re.search(r"SQLSTATE:?\s*([0-9A-Z]+)", log or "")
    if ec and s:
        return f"{ec} (SQLSTATE {s.group(1)})"
    return ec


# ---------------------------------------------------------------------------
# Agent brain (Anthropic)
# ---------------------------------------------------------------------------
class AnthropicBrain(AgentBrain):
    """Drives one proposal per turn via the Anthropic Messages API.

    The system prompt is assembled from: the shared task prompt, the arm's
    paradigm framing, the (optionally) linked skill packs, and the running
    feedback. Output is parsed into a Proposal (code + command). Token usage is
    accumulated for cost reporting (LLM $ is tracked separately from cluster $).
    """

    def __init__(self, base_model_id: str, task_prompt: str,
                 temperature: float = 0.0, top_p: float = 1.0,
                 sampling: Optional[dict] = None,
                 omnigent_skills_dir: Optional[str] = None,
                 max_tokens: int = 32000,
                 bounded: bool = True,
                 propose_timeout_s: float = PROPOSE_WALL_TIMEOUT_S):
        self.name = "anthropic"
        # bounded=True (the sweep default) runs each messages.create in a KILLABLE
        # subprocess under `propose_timeout_s`, the harness-owned hard wall-clock bound
        # (deliverable 3). bounded=False keeps the simple in-process call (used only
        # where the caller already owns isolation). propose_timeout_s is injectable so
        # tests can drive the hang path without waiting the production backstop.
        self.bounded = bounded
        self.propose_timeout_s = propose_timeout_s
        self.base_model_id = base_model_id
        self.task_prompt = task_prompt
        # The sampling kwargs actually SENT to the API. Claude 4.x rejects sending
        # BOTH temperature and top_p (hard 400), so we transmit only the controlled
        # determinism knob. `sampling` (from arm_manifest.sampling_kwargs) is the
        # single source of truth; the temperature/top_p args are a fallback that
        # builds the same one-knob dict. NEVER assemble both into the request.
        if sampling is not None:
            self.sampling = dict(sampling)
        else:
            self.sampling = {"temperature": temperature}  # top_p left as the API default
        self.temperature = temperature
        self.top_p = top_p
        # Skill packs: explicit arg > OMNIGENT_SKILLS env > the in-repo default
        # (resolved relative to the study dir). The env/config being unset is the
        # COMMON case here, so without the default the SDP arms (B/B1) would load NO
        # SKILL.md and the agents hallucinate Databricks dlt -- the zero-completion
        # root cause. The default makes `_load_skill` find the in-repo packs.
        self.omnigent_skills_dir = (
            omnigent_skills_dir or os.environ.get("OMNIGENT_SKILLS") or _DEFAULT_SKILLS_DIR)
        # max_tokens=32000 (raised 8000 -> 16000 -> 32000): the opus path uses ADAPTIVE
        # thinking at `effort=high`, and thinking SHARES the max_tokens budget. At 16000 it
        # still routinely exhausted the budget on reasoning BEFORE the fenced code module
        # emitted -> empty `proposal.code` -> a no-code iteration (DEVIATIONS D-7), acute on
        # the no-safety arm whose deliberation runs longer without the skill's idioms.
        # 32000 gives ample room for deep thinking + a code module. This EXCEEDS the SDK's
        # ~16K non-streaming long-request guard, so `_messages_create` now uses the STREAMING
        # path (per-chunk timeout, no total-time ValueError); see its docstring.
        self.max_tokens = max_tokens
        self.input_tokens = 0
        self.output_tokens = 0
        self._client = None
        # Consecutive TRANSIENT API failures within THIS cell (the brain is rebuilt per
        # cell), used to escalate a persistent outage to an AgentApiError. Reset on any
        # successful call.
        self._consecutive_api_failures = 0

    def build_request(self, system: str, messages: list) -> dict:
        """Assemble the exact kwargs for client.messages.create().

        Two model families, two request shapes (exposed, no network, so tests can
        assert the shape for every arm):

        * claude-opus-4-x: REJECTS `temperature`/`top_p`/`top_k`/`budget_tokens`
          with a hard 400 and uses ADAPTIVE thinking. We transmit NO sampling knob
          and set `thinking={'type':'adaptive'}` (+ `output_config.effort=high`).
          The arm manifests still RECORD temperature/top_p as controlled-variable
          provenance (`self.sampling`); we simply do not send them.
        * other Claude 4.x (e.g. claude-sonnet-4-6): send AT MOST ONE of
          {temperature, top_p} (self.sampling), so the 'both specified' 400 cannot
          occur.
        """
        req = {
            "model": self.base_model_id,
            "max_tokens": self.max_tokens,
            "system": system,
            "messages": messages,
        }
        if _is_adaptive_thinking_model(self.base_model_id):
            req["thinking"] = {"type": "adaptive"}
            req["output_config"] = {"effort": "high"}
            # deliberately transmit NO temperature/top_p/top_k/budget_tokens.
        else:
            req.update(self.sampling)
        return req

    def _client_lazy(self):
        if self._client is None:
            # SSOT bounds: a per-request HTTP timeout (env-overridable, default RAISED to
            # 300 so a stalled response can't block propose() forever -- the calibration
            # hang -- but slow-but-successful heavy turns are no longer killed mid-flight)
            # plus a few SDK retries for transient 429/5xx/network blips. Built through
            # `_build_anthropic_client` so the in-process client and the killable
            # subprocess worker share identical bounds. Reads ANTHROPIC_API_KEY.
            self._client = _build_anthropic_client()
        return self._client

    def _load_skill(self, skill_name: str) -> str:
        """Load a linked skill's SKILL.md text, if present, for the system prompt."""
        if not self.omnigent_skills_dir:
            return ""
        for cand in (
            os.path.join(self.omnigent_skills_dir, skill_name, "SKILL.md"),
            os.path.join(self.omnigent_skills_dir, skill_name, "skill.md"),
        ):
            if os.path.exists(cand):
                with open(cand) as f:
                    return f.read()
        return ""

    def _system_prompt(self, arm: Any) -> str:
        parts: List[str] = [self.task_prompt.strip()]
        if arm.paradigm == "sdp":
            parts.append(
                "PARADIGM: Author this as a Spark Declarative Pipeline "
                "(`from pyspark import pipelines as dp`; @dp.table / "
                "@dp.materialized_view). Declare dependencies via table reads; do "
                "not call .start()/.awaitTermination() yourself."
            )
        else:
            parts.append(
                "PARADIGM: Author this as imperative pyspark.sql / Structured "
                "Streaming. You own the SparkSession and the read/transform/write.\n"
                "Your module `pipeline.py` must be runnable as a standalone program:\n"
                "  (1) it acquires its OWN SparkSession;\n"
                "  (2) it reads its primary input from the environment variable "
                "AGENT_INPUT_PATH (fall back to the dataset path given below if that "
                "variable is unset);\n"
                "  (3) it writes the final GOLD output to the environment-provided "
                "contract location (AGENT_OUTPUT_PATH parquet path when present; "
                "otherwise AGENT_OUTPUT_TABLE for remote/table-backed runs);\n"
                "  (4) on success it prints the exact line `Run is COMPLETED` to "
                "stdout."
            )
        for skill in getattr(arm, "skills", []):
            text = self._load_skill(skill)
            if text:
                parts.append(f"=== LINKED SKILL: {skill} ===\n{text}")
        if getattr(arm, "dry_run_gate", False):
            parts.append(
                "GATE: A structural dry-run gate screens every proposal before it "
                "executes. Use it: prefer proposals that pass structural analysis."
            )
        parts.append(
            "OUTPUT CONTRACT: respond with a fenced ```python block (the full "
            "transform module) followed by a line `COMMAND: <one of the allowed "
            f"commands>`. Allowed commands: {getattr(arm, 'allowed_commands', [])}."
        )
        return "\n\n".join(parts)

    def _user_message(self, state: LoopState, arm: Any) -> str:
        msg = [f"Task: {state.task}", f"Workspace: {state.workspace}",
               f"Dataset (NDJSON, this seed): {state.dataset_path}"]
        # Multi-input tasks: the SAME strictly-location-only aux block for BOTH
        # paradigms (name + staged path only; no env-var doc, no format, no how-to --
        # prompt-no-leak). It mirrors the primary 'Dataset: <path>' line above.
        aux_block = aux_locations_text(getattr(state, "aux_inputs", None) or {})
        if aux_block:
            msg.append(aux_block)
        if getattr(arm, "paradigm", None) != "sdp" and state.output_path:
            if state.dedup_path:
                # SYMMETRIC, defect-neutral disk pin for the additional contract table:
                # location only, exactly like the primary gold output above. The
                # semantic target is unchanged; we only pin WHERE it lands so its grade
                # survives this program calling spark.stop() (a temp view would not).
                # No table name / purpose / defect label is leaked -- the agent maps the
                # single extra contract table to the single AGENT_DEDUP_PATH slot.
                msg.append(
                    f"Part-1 LOCAL imperative output contract: write the final gold "
                    f"DataFrame as parquet to AGENT_OUTPUT_PATH={state.output_path}. For "
                    f"the additional table this task's contract requires you to "
                    f"materialize, write its DataFrame as parquet to "
                    f"AGENT_DEDUP_PATH={state.dedup_path} -- a real on-disk dataset, NOT "
                    f"only a temp view. Keep intermediate stages as DataFrames/temp "
                    f"views; do NOT call saveAsTable.")
            else:
                msg.append(
                    f"Part-1 LOCAL imperative output contract: write the final gold DataFrame "
                    f"as parquet to AGENT_OUTPUT_PATH={state.output_path}. Keep intermediate "
                    f"stages as DataFrames/temp views; do NOT call saveAsTable. For ANY other "
                    f"table this task's contract requires you to materialize, register its "
                    f"DataFrame in the same SparkSession as a TEMP VIEW with exactly that "
                    f"contract name using createOrReplaceTempView.")
        if state.feedback:
            msg.append("Feedback from previous iterations (most recent last):")
            msg.extend(f"  - {fb}" for fb in state.feedback[-6:])
        else:
            msg.append("This is the first iteration. Profile the data, then write the pipeline.")
        return "\n".join(msg)

    def propose(self, state: LoopState, arm: Any) -> Proposal:
        req = self.build_request(
            system=self._system_prompt(arm),
            messages=[{"role": "user", "content": self._user_message(state, arm)}],
        )
        # Two crash-safety paths for the uncontrolled, network-bound LLM step. They
        # COMPOSE the two parallel hardening lines:
        #
        #  * BOUNDED subprocess (#31, the SWEEP DEFAULT): run messages.create in a KILLABLE
        #    subprocess under the harness-owned wall-clock bound. A failure raises a
        #    ProposeError (PROPOSE_TIMEOUT / PROPOSE_RATE_LIMIT / PROPOSE_API_ERROR) and a
        #    HANG is SIGKILLed at the process group -- the runner's per-cell net converts
        #    either into a soft-failed cell, NEVER propagating to abort the batch. Used in
        #    production (no in-process client is built) and by the propose-hardening tests.
        #
        #  * IN-PROCESS (#40 lineage): the simple call through the in-process SSOT client,
        #    with the transient-degradation policy -- an EPHEMERAL post-retry blip degrades
        #    to a no-code turn (the loop retries) and a PERSISTENT outage escalates to
        #    AgentApiError (a bounded harness_error row) rather than wedging or being
        #    mis-attributed to the arm. Used when bounded=False OR when a caller/test has
        #    already supplied an in-process client (`self._client`) it expects to drive.
        #
        # Selecting in-process whenever a client is already present keeps the in-process
        # degradation contract intact (callers that inject `_client`) while leaving the
        # bounded subprocess as the production/default path. Both paths yield the SAME
        # `result` dict shape (text / tokens / stop_reason).
        use_inprocess = (not self.bounded) or (self._client is not None)
        if not use_inprocess:
            result = self._propose_bounded(req)
        else:
            try:
                client = self._client_lazy()
                resp = client.messages.create(**req)
                result = _result_from_response(resp)
            except (KeyboardInterrupt, SystemExit):
                # NEVER swallow these -- let an operator's Ctrl-C / a clean shutdown through.
                raise
            except Exception as e:  # noqa: BLE001 -- API/SDK/network errors handled below
                if _is_transient_api_error(e):
                    self._consecutive_api_failures += 1
                    if self._consecutive_api_failures >= TRANSIENT_API_FAILURE_LIMIT:
                        # Persistent outage, not a blip: stop attributing it to the agent.
                        raise AgentApiError(
                            f"[API_ERROR] {self._consecutive_api_failures} consecutive "
                            f"transient Anthropic API failures in this cell; last "
                            f"{type(e).__name__}: {e}") from e
                    # Ephemeral timeout/connection AFTER the SDK's own retries: degrade to
                    # a no-code turn so the loop can try again next iteration. This is NOT a
                    # model/infra FAILURE attribution -- the no-code guard just retries.
                    print(f"[anthropic] transient request failure "
                          f"({type(e).__name__}: {e}); retrying as a no-code iteration "
                          f"({self._consecutive_api_failures}/{TRANSIENT_API_FAILURE_LIMIT}).",
                          file=sys.stderr)
                    return Proposal(iteration=len(state.history), code="", command="",
                                    rationale="transient anthropic failure",
                                    stop_reason="api_error")
                # NON-transient (auth / quota / bad-request / validation / SDK / unexpected):
                # an INFRA/MODEL failure, NOT an agent no-code turn. Surface it so the cell
                # is recorded as a bounded harness_error row, distinguishable from a real
                # arm/task outcome (never max_iterations, never task_success).
                raise AgentApiError(f"[API_ERROR] non-transient Anthropic API failure: "
                                    f"{type(e).__name__}: {e}") from e
        # token/usage accounting is preserved across the success path (and across the
        # subprocess boundary): the worker serializes usage back and we accumulate it.
        self._consecutive_api_failures = 0  # a successful call clears the streak
        in_tok = int(result.get("input_tokens", 0) or 0)
        out_tok = int(result.get("output_tokens", 0) or 0)
        self.input_tokens += in_tok
        self.output_tokens += out_tok
        stop_reason = result.get("stop_reason")
        text = result.get("text", "")
        code, command = _parse_proposal(text, arm)
        if not code.strip():
            # VISIBILITY: a turn that yielded no fenced code is almost always an
            # adaptive-thinking budget truncation (stop_reason="max_tokens"). Surface
            # it so the run-loop's no-code guard records WHY (and so a recurring
            # truncation is debuggable from the transcript), rather than a silent empty.
            print(f"[anthropic] empty code block (stop_reason={stop_reason!r}, "
                  f"output_tokens~{result.get('output_tokens', '?')}); "
                  f"agent will be asked to retry.", file=sys.stderr)
        # carry the PER-TURN usage out on the Proposal so the runner persists it into the
        # per-iteration transcript + the ResultRow totals (Part A.5).
        return Proposal(iteration=len(state.history), code=code, command=command,
                        rationale="anthropic turn", stop_reason=stop_reason,
                        input_tokens=in_tok, output_tokens=out_tok)

    # -- HARNESS-OWNED hard cancellation (deliverable 3) ----------------------
    def _propose_bounded(self, req: dict) -> dict:
        """Run ONE messages.create in a KILLABLE subprocess under a hard wall-clock
        bound the HARNESS owns. Mirrors ConnectExecutor._run: the child is started in
        its OWN process group (`start_new_session=True`) so that, on timeout, killing
        the GROUP (`os.killpg(..., SIGKILL)`) reaps the python launcher AND any thread
        stuck in a C-level socket read -- the exact failure the SDK timeout can miss.
        The request kwargs go in over stdin; the projected result comes back as JSON on
        stdout. Returns the result dict, or raises a ProposeError the runner fails soft:

          * subprocess.TimeoutExpired  -> kill the group, raise ProposeTimeout
          * worker reports an API error -> ProposeRateLimited (429) / ProposeApiError
          * worker crashed / unparseable -> ProposeApiError
        """
        worker = [sys.executable, "-m", "harness.backends.propose_worker"]
        env = dict(os.environ)
        env["PYTHONPATH"] = _worker_pythonpath(env.get("PYTHONPATH"))
        # the worker only needs stdin/stdout/stderr; isolate it in its own session so
        # the whole group is killable as one unit (launcher + any JVM/helper it spawns).
        proc = subprocess.Popen(
            worker, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True, env=env)
        try:
            out, err = proc.communicate(input=json.dumps(req), timeout=self.propose_timeout_s)
        except subprocess.TimeoutExpired:
            # the call hung past the harness bound: SIGKILL the WHOLE group, then reap.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:  # noqa: BLE001
                proc.kill()
            try:
                proc.communicate(timeout=15)
            except Exception:  # noqa: BLE001
                pass
            raise ProposeTimeout(
                f"propose() exceeded the harness wall-clock bound "
                f"({self.propose_timeout_s:.0f}s); child process group was killed")
        if proc.returncode != 0:
            # the worker itself died (not a reported API error): surface as an API error.
            raise ProposeApiError(
                f"propose worker exited {proc.returncode}: {(err or '').strip()[:500]}")
        try:
            result = json.loads(out)
        except Exception as e:  # noqa: BLE001
            raise ProposeApiError(
                f"propose worker returned unparseable output: {type(e).__name__}: {e}; "
                f"stdout[:200]={out[:200]!r} stderr[:200]={(err or '')[:200]!r}")
        if not result.get("ok", False):
            raise _classify_worker_error(result)
        return result


def _parse_proposal(text: str, arm: Any):
    import re
    m = re.search(r"```python\s*(.*?)```", text, re.DOTALL)
    code = m.group(1).strip() if m else ""
    cmd_m = re.search(r"COMMAND:\s*(.+)", text)
    command = cmd_m.group(1).strip() if cmd_m else ""
    allowed = getattr(arm, "allowed_commands", [])
    if allowed and command not in allowed:
        # snap to the safest allowed command rather than running something off-policy
        command = allowed[0]
    return code, command


# ---------------------------------------------------------------------------
# Spark Connect executor
# ---------------------------------------------------------------------------
class ConnectExecutor(SparkExecutor):
    """Runs the structural gate and executes proposals against Spark Connect.

    * structural gate, SDP arm:        `pipelines/cli.py dry-run` (driver-only).
    * structural gate, imperative arm: the AGENT'S `pipeline.py --analyze-only`
      (the agent builds + analyzes its plan WITHOUT materialization; no executors).
    * execute, SDP arm:                `pipelines/cli.py run --spec` (mirrors the
      gate over SPARK_REMOTE; not the `bin/spark-pipelines` wrapper -- see #15).
    * execute, imperative arm:         the AGENT'S chosen command
      (`python3 pipeline.py` / `spark-submit pipeline.py`) on the agent's program,
      with NEUTRAL env (AGENT_INPUT_PATH / AGENT_OUTPUT_TABLE / SPARK_REMOTE for remote;
      Part-1 LOCAL uses AGENT_OUTPUT_PATH instead). The
      harness injects NO SparkSession or main; the agent owns acquisition, read,
      transform, write, and the COMPLETED signal. Completion is then verified by a
      neutral read-back of the contract output table (DEVIATIONS D-4).

    Compute attribution (H2) -- STAGE DIFF, not `/executors.totalDuration` (D-5):
    this cluster has ONE long-lived 'Spark Connect server' application whose
    `/executors` `totalDuration` is app/driver UPTIME and INCREMENTS WHILE IDLE, so a
    before/after delta of it does NOT attribute per-run task compute. Instead we
    snapshot the set of stageIds from `/applications/<app_id>/stages` BEFORE the run
    and, AFTER, attribute the NEW stages that reached `status == 'COMPLETE'` during
    the window:
      executor_seconds = sum(stage.executorRunTime)/1000   (executor wall-time, ms)
      cpu_seconds      = sum(stage.executorCpuTime)/1e9     (executor CPU-time, ns)
    The agent's Spark job runs in a SUBPROCESS with its OWN Connect session, so a
    job tag can't cross sessions -- but the stage diff can, because the harness runs
    cells SEQUENTIALLY, so the stages that newly COMPLETE in this window belong to
    this run. Validated live: a subprocess `spark.range(80M).sum()` produced new
    stages [60,62] -> executor_seconds 1.246, cpu_seconds 0.878. If `spark_rest_url`
    is unset or any REST call fails we fall back GRACEFULLY to (None, None) and the
    cost model derives the wall-clock x slots estimate (never crashes the run).
    """

    def __init__(self, spark_remote: str, spark_rest_url: Optional[str] = None,
                 staging_base: Optional[str] = None,
                 cmd_timeout_s: float = CONNECT_CMD_TIMEOUT_S):
        self.name = "connect"
        self.spark_remote = spark_remote
        self.spark_rest_url = spark_rest_url
        # hard wall-clock bound on each gate/execute COMMAND (SDP CLI or imperative
        # program over Connect): on timeout the child process GROUP is killed and the
        # iteration fails with EXECUTION_TIMEOUT, so a hung remote/CLI run can't wedge.
        self.cmd_timeout_s = cmd_timeout_s
        # cluster-reachable base (e.g. s3a://.../warehouse/_ssa_staging/<task>/<arm>/<seed>)
        # where this cell's input NDJSON is staged so the REMOTE k8s executors can read it.
        self.staging_base = staging_base
        self._spark = None
        # the long-lived 'Spark Connect server' app id, resolved once from the driver
        # REST and cached (it does not change for the life of this executor).
        self._cached_app_id: Optional[str] = None

    @property
    def spark(self):
        """A Connect session for staging input + reading materialized tables back."""
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.remote(self.spark_remote).getOrCreate()
        return self._spark

    def read_table(self, name: str):
        """Read a materialized table back through the Connect session (B1)."""
        return self.spark.table(name)

    def stage_input(self, local_path: str, subkey: Optional[str] = None) -> str:
        """Stage the per-seed input to executor-readable S3 over Connect (D-3).

        The remote k8s executors cannot see this machine's filesystem, so a bare
        `file:/local/...` input is PATH_NOT_FOUND. `copyFromLocalToFs` is NOT usable
        here: it always writes to the DRIVER pod's default filesystem (this cluster's
        driver default scheme is `file:`), so a scheme'd `s3a://` dest is rejected and
        a scheme-less dest lands on driver-local disk the executors can't read.

        Instead we ship the rows to the cluster over the Connect protocol and let the
        executors write S3 via IRSA (proven live):
          1. read the local NDJSON lines in the Python client;
          2. createDataFrame([(line,)], "value string")  -- rows travel client->driver
             over Connect, NOT a shared filesystem;
          3. df.write.text(s3_dest)  -- executors write S3 natively (IRSA), no local
             AWS creds, no copyFromLocalToFs.
        `.write.text()` of the single `value` column emits files whose lines are the
        original NDJSON objects, so the staged location is still valid NDJSON:
        `spark.read.text(...)` (oracles) and `spark.read.json(...)` (agent) both read
        it correctly. Input is small (~5.3k lines / 636K) -> one createDataFrame, no
        chunking. Returns the s3_dest directory the agent + oracle read.
        """
        if not self.staging_base:
            return local_path
        # AUX inputs land under their own subkey so multiple staged inputs of one
        # cell never overwrite each other; the primary (subkey=None) keeps the
        # original staging base, so single-input arms stage byte-for-byte as before.
        s3_dest = self.staging_base if subkey is None else f"{self.staging_base.rstrip('/')}/{subkey}"
        with open(local_path) as f:
            lines = f.read().splitlines()
        df = self.spark.createDataFrame([(line,) for line in lines], "value string")
        df.write.mode("overwrite").text(s3_dest)
        return s3_dest

    def reachable(self) -> bool:
        """Best-effort check that the Connect backend answers. Never raises."""
        try:
            host_port = self.spark_remote.split("//", 1)[-1].split("/", 1)[0]
            host, _, port = host_port.partition(":")
            import socket
            with socket.create_connection((host, int(port or "15002")), timeout=3):
                return True
        except Exception:
            return False

    def _run(self, argv: List[str], cwd: str, env_extra: dict) -> tuple:
        """Run a gate/execute COMMAND under a hard timeout. The child is started in its
        OWN process group (`start_new_session=True`) so that, on timeout, killing the
        group reaps the CLI/python launcher AND its JVM child -- nothing leaks. On
        timeout returns rc=124 with an `[EXECUTION_TIMEOUT]` log (the bracket token the
        error-class extractor reads), so a hung remote/CLI run becomes a graceful failed
        iteration instead of wedging the run. Returns (rc, log, wall)."""
        import signal
        import subprocess
        env = dict(os.environ)
        env["SPARK_REMOTE"] = self.spark_remote
        env.update(env_extra)
        t0 = time.time()
        proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            out, err = proc.communicate(timeout=self.cmd_timeout_s)
            wall = time.time() - t0
            return proc.returncode, (out + "\n" + err), wall
        except subprocess.TimeoutExpired:
            # kill the WHOLE process group (CLI/launcher + JVM), then reap.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:  # noqa: BLE001
                proc.kill()
            try:
                out, err = proc.communicate(timeout=15)
            except Exception:  # noqa: BLE001
                out, err = "", ""
            wall = time.time() - t0
            return 124, (f"{out}\n{err}\n[EXECUTION_TIMEOUT] command exceeded "
                         f"{self.cmd_timeout_s:.0f}s and was killed."), wall

    def _imperative_env(self, state: LoopState) -> dict:
        """NEUTRAL env for the agent-owned imperative program: the input path and
        the contract output-table name. SPARK_REMOTE is added by `_run`. No session
        or main code is injected -- the agent owns the program (DEVIATIONS D-4).

        Multi-input tasks additionally get their declared AUX inputs as a JSON
        name->path map in AGENT_AUX_INPUTS (+ per-name AGENT_AUX_INPUT_<NAME>); when
        the task has none the env is byte-for-byte the original single-input env."""
        env = {"AGENT_INPUT_PATH": state.dataset_path,
               "AGENT_OUTPUT_TABLE": state.output_table}
        env.update(aux_input_env(state))
        return env

    def _imperative_execute_argv(self, proposal: Proposal, arm: Any,
                                 spark_home: str, state: LoopState) -> List[str]:
        """Run the agent's CHOSEN command on the agent artifact `pipeline.py`:
        `spark-submit pipeline.py` or `python3 pipeline.py` per the proposal's
        COMMAND (already snapped into arm.allowed_commands by `_parse_proposal`)."""
        pipeline = os.path.join(state.workspace, "pipeline.py")
        cmd = (proposal.command or "").strip()
        if "spark-submit" in cmd:
            return [os.path.join(spark_home, "bin", "spark-submit"), pipeline]
        return ["python3", pipeline]

    def _imperative_gate_argv(self, state: LoopState) -> List[str]:
        """B2 gate: the AGENT'S program in `--analyze-only` mode (no harness session,
        no harness-created _analyze_only.py)."""
        return ["python3", os.path.join(state.workspace, "pipeline.py"), "--analyze-only"]

    def _table_readable(self, name: str):
        """Neutral completion check: is the contract output table readable? Reads a
        limit-0 slice back through the Connect session. Returns (ok, reason)."""
        try:
            self.read_table(name).limit(0).collect()
            return True, ""
        except Exception as e:  # noqa: BLE001
            return False, f"output table {name!r} not readable: {type(e).__name__}: {e}"

    def _missing_agent_artifact(self, arm: Any, state: LoopState) -> Optional[str]:
        """The file this executor will open per paradigm: `spark-pipeline.yml` (SDP)
        or `pipeline.py` (imperative). Returns a reason string if it is MISSING (an
        empty agent proposal wrote nothing), else None. DEFENSIVE: the run-loop guards
        the no-code case first, but the executor must never crash a run by handing a
        non-existent path to the CLI/subprocess."""
        rel = "spark-pipeline.yml" if arm.paradigm == "sdp" else "pipeline.py"
        if not os.path.exists(os.path.join(state.workspace, rel)):
            return f"[NO_CODE_PRODUCED] agent wrote no {rel} (empty proposal)"
        return None

    def _sdp_spec_path(self, state: LoopState) -> str:
        """The ABSOLUTE `--spec` path for the SDP CLI (Part A.1/A.2 root-cause fix).

        The original bug: `--spec` was a RELATIVE `os.path.join(workspace, 'spark-
        pipeline.yml')` while `_run` set the subprocess cwd to that SAME workspace, so
        the CLI resolved the spec against its cwd and got the DOUBLED path
        `<workspace>/<workspace>/spark-pipeline.yml` -> PIPELINE_SPEC_FILE_DOES_NOT_EXIST,
        and EVERY SDP iteration failed before the agent's code ran. Building the spec
        with `os.path.abspath` makes it cwd-independent, so the doubled path cannot
        recur regardless of what cwd `_run` uses."""
        return os.path.abspath(os.path.join(state.workspace, "spark-pipeline.yml"))

    def _require_sdp_spec(self, state: LoopState) -> str:
        """Return the absolute spec path, or raise a HARNESS FAULT (NOT an agent
        failure) if it is missing (Part A.2). The agent-artifact check already ran, so
        a missing ABSOLUTE spec here means the INSTRUMENT is broken (a path bug, a
        failed write) -- exactly the failure mode that previously masqueraded as an
        agent PIPELINE_SPEC_FILE_DOES_NOT_EXIST. Surfacing it as a HarnessFault keeps a
        broken instrument out of the agent statistics."""
        spec = self._sdp_spec_path(state)
        if not os.path.isfile(spec):
            raise HarnessFault(
                f"SDP spec not found at the absolute path {spec!r} after "
                f"materialization (workspace={state.workspace!r}); the harness, not the "
                f"agent, failed to provide a resolvable --spec.",
                reason="SDP_SPEC_MISSING")
        return spec

    def run_gate(self, proposal: Proposal, arm: Any, state: LoopState) -> GateOutcome:
        missing = self._missing_agent_artifact(arm, state)
        if missing:
            return GateOutcome(failed=True, wall_s=0.0,
                               error_class="NO_CODE_PRODUCED", log=f"gate: {missing}")
        spark_home = os.environ.get("SPARK_HOME") or _spark_home()
        cwd = os.path.abspath(state.workspace)   # explicit ABSOLUTE cwd (Part A.2)
        if arm.paradigm == "sdp":
            # Graph-aware SDP dry-run gate (harness/sdp_dryrun.py): pre-seeds sibling
            # dataset schemas before the authoritative `dry-run` so windowed/dedup
            # reads of a SIBLING pipeline dataset resolve against the dataflow graph,
            # not the bare session catalog (the local-only TABLE_OR_VIEW_NOT_FOUND /
            # 42P01 eager-analysis trap). It runs the REAL SDP dry-run for the verdict
            # and never seeds the execute path, so it stays exactly as strict as real
            # SDP/DLT (missing upstream / unresolved column still fail). See its module
            # docstring. DRY-RUN ONLY -- the execute path below stays on the stock CLI.
            driver = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "sdp_dryrun.py")
            # ABSOLUTE --spec via _require_sdp_spec: abspath (cwd-independent, so the
            # workspace-doubling PIPELINE_SPEC_FILE_DOES_NOT_EXIST cannot recur) PLUS an
            # isfile check that raises a HarnessFault (SDP_SPEC_MISSING) -- a missing spec
            # after materialization is a broken INSTRUMENT, not an agent failure, and is
            # kept out of the agent statistics rather than masquerading as one.
            spec = self._require_sdp_spec(state)
            argv = ["python3", driver, "--spec", spec]
            env_extra: dict = {}
        else:
            # imperative analyze-only: run the AGENT'S program with --analyze-only,
            # which builds + analyzes its plan WITHOUT materializing (no executors).
            argv = self._imperative_gate_argv(state)
            env_extra = self._imperative_env(state)
        rc, log, wall = self._run(argv, cwd, env_extra)
        failed = rc != 0
        return GateOutcome(failed=failed, wall_s=wall,
                           error_class=extract_error_class(log) if failed else None,
                           log=log)

    def run_execute(self, proposal: Proposal, arm: Any, state: LoopState) -> ExecOutcome:
        missing = self._missing_agent_artifact(arm, state)
        if missing:
            return ExecOutcome(failed=True, completed=False, wall_s=0.0,
                               executor_seconds=None, cpu_seconds=None,
                               error_class="NO_CODE_PRODUCED", log=f"execute: {missing}")
        spark_home = os.environ.get("SPARK_HOME") or _spark_home()
        cwd = os.path.abspath(state.workspace)   # explicit ABSOLUTE cwd (Part A.2)
        if arm.paradigm == "sdp":
            # Mirror the (working) SDP gate: invoke the Python CLI directly with
            # --spec (an ABSOLUTE path -- Part A.1/A.2), relying on the SPARK_REMOTE env
            # that _run sets. The `bin/spark-pipelines` wrapper is NOT usable here -- it
            # ignores SPARK_REMOTE and tries to start a LOCAL Connect server (binds
            # :15002, fails), and without --spec it auto-discovers only pipeline.yml/
            # .yaml, never the spark-pipeline.yml the harness writes.
            cli = os.path.join(spark_home, "pipelines", "cli.py")
            # ABSOLUTE --spec via _require_sdp_spec (same cwd-doubling fix as the gate,
            # plus the isfile -> HarnessFault instrument-validity check).
            spec = self._require_sdp_spec(state)
            argv = ["python3", cli, "run", "--spec", spec]
            env_extra: dict = {}
        else:
            # the agent OWNS the program: run its chosen command on pipeline.py with
            # NEUTRAL env only (AGENT_INPUT_PATH / AGENT_OUTPUT_TABLE; SPARK_REMOTE via
            # _run). No harness session/main is injected (DEVIATIONS D-4).
            argv = self._imperative_execute_argv(proposal, arm, spark_home, state)
            env_extra = self._imperative_env(state)
        # D-5: per-run compute via a before/after STAGE DIFF (NOT /executors
        # totalDuration, which is idle-incrementing driver uptime on the shared
        # long-lived Connect app). Snapshot the existing stageIds, run the cell,
        # then attribute the stages that NEWLY COMPLETED during the window. Applied
        # UNIFORMLY here for BOTH the SDP and imperative branches (H2 compares them,
        # so they must be measured identically). Relies on SEQUENTIAL cell execution.
        before_ids = self._stage_ids_snapshot()
        rc, log, wall = self._run(argv, cwd, env_extra)
        exec_s, cpu_s = self._stage_compute_since(before_ids)
        failed = rc != 0
        completed = (not failed) and ("COMPLETED" in log or rc == 0)
        error_class = extract_error_class(log) if failed else None
        if arm.paradigm != "sdp" and not failed:
            # NEUTRAL completion check (replaces the old harness saveAsTable): the
            # agent owns materialization, so we VERIFY the contract output table is
            # readable rather than trusting harness-written Spark. Missing/unreadable
            # -> a failed, non-completed run with a structural error class.
            ok, why = self._table_readable(state.output_table)
            if not ok:
                failed, completed, error_class = True, False, "OUTPUT_TABLE_NOT_FOUND"
                log = f"{log}\n[completion-check] {why}"
        # the agent's output table name comes from the task's output contract;
        # the runner reads it back via read_table -> output oracle (B1).
        return ExecOutcome(
            failed=failed,
            completed=completed,
            wall_s=wall,
            executor_seconds=exec_s,
            cpu_seconds=cpu_s,
            error_class=error_class,
            log=log,
            output_tables=[],          # discovered by the runner from the task contract
            output_metrics=None,       # real path reads the table; no canned metrics
        )

    def _app_id(self) -> Optional[str]:
        """Resolve & cache the long-lived 'Spark Connect server' app id from the
        driver REST (`/applications` -> [0]['id']). None if the REST is unset or
        unreachable (-> graceful (None, None) compute, never a crash)."""
        if self._cached_app_id is not None:
            return self._cached_app_id
        if not self.spark_rest_url:
            return None
        try:
            apps = _get_json(f"{self.spark_rest_url}/api/v1/applications")
            if not apps:
                return None
            self._cached_app_id = apps[0]["id"]
            return self._cached_app_id
        except Exception:
            return None

    def _stage_ids_snapshot(self) -> Optional[set]:
        """The set of stageIds the driver currently knows, for a before/after diff.

        Returns None (NOT an empty set) when the REST is unset/unreachable, so the
        caller can distinguish "no metrics available" (-> fall back to the wall-clock
        estimate) from "nothing had run yet" (a real empty set).

        STAGE-RETENTION caveat (see also `_stage_compute_since`): the Spark UI keeps
        only the most recent `spark.ui.retainedStages` (default 1000) stages, so if
        the before-set is already near that bound, old stageIds can be EVICTED during
        the run window and the after-diff could undercount. The study's per-cell
        pipelines create only a handful of stages each, so this is not a practical
        risk; we still warn here if the snapshot is suspiciously close to the default
        bound so a misconfigured cluster is noticed rather than silently undercounted."""
        app_id = self._app_id()
        if app_id is None:
            return None
        try:
            stages = _get_json(f"{self.spark_rest_url}/api/v1/applications/{app_id}/stages")
            ids = {s["stageId"] for s in stages}
            if len(ids) >= _STAGE_RETENTION_WARN_AT:
                import warnings
                warnings.warn(
                    f"Spark UI is tracking {len(ids)} stages (near the default "
                    f"spark.ui.retainedStages=1000 bound): old stages may be evicted "
                    f"mid-run and the stage-diff compute could undercount. Raise "
                    f"spark.ui.retainedStages on the Connect server.", RuntimeWarning)
            return ids
        except Exception:
            return None

    def _stage_compute_since(self, before_ids: Optional[set]):
        """(executor_seconds, cpu_seconds) attributed to stages that COMPLETED during
        this run window: NEW stageIds (absent from `before_ids`) whose status is
        'COMPLETE'. This is the D-5 replacement for the invalid
        `/executors.totalDuration` delta.

          executor_seconds = sum(executorRunTime)/1000.0   (ms -> s)
          cpu_seconds      = sum(executorCpuTime)/1e9       (ns -> s)

        Returns (None, None) if the before-snapshot was unavailable or any REST call
        fails, so the run never crashes and cost.py derives the wall-clock estimate.
        Assumes SEQUENTIAL cell execution (holds for the controlled sweep): the only
        stages that newly COMPLETE in this window belong to THIS run, even though the
        agent's Spark job runs in a subprocess with its own Connect session.

        Assumes the run's stages are still RETAINED in the UI at the after-snapshot:
        it sums only stages `/stages` still returns, so a run that creates more
        completed stages than `spark.ui.retainedStages` (default 1000) between the
        BEFORE and AFTER snapshots could undercount. The study's small per-cell
        pipelines create few stages, so this is not a practical risk; size
        `spark.ui.retainedStages` above the largest per-cell stage count to be safe
        (`_stage_ids_snapshot` warns if the live count nears the default bound)."""
        if before_ids is None:
            return None, None
        app_id = self._app_id()
        if app_id is None:
            return None, None
        try:
            stages = _get_json(f"{self.spark_rest_url}/api/v1/applications/{app_id}/stages")
        except Exception:
            return None, None
        new_complete = [s for s in stages
                        if s.get("stageId") not in before_ids and s.get("status") == "COMPLETE"]
        exec_s = sum(float(s.get("executorRunTime", 0)) for s in new_complete) / 1000.0
        cpu_s = sum(float(s.get("executorCpuTime", 0)) for s in new_complete) / 1e9
        return exec_s, cpu_s


def _spark_home() -> str:
    import pyspark
    return os.path.dirname(pyspark.__file__)


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as r:
        import json
        return json.loads(r.read().decode())
