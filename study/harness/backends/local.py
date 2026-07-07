"""Local real-Spark backend: deterministic brain + in-process Spark executor.

This is the LIVE measurement path exercised WITHOUT a remote cluster or an API
key, so the instrument's real grading/cost path is provable in CI.

Part-1 LOCAL imperative contract (A/B2): the agent owns its program and final
write, but the final GOLD output is path-based parquet, not a catalog table. The
executor provides AGENT_OUTPUT_PATH, runs the agent in-process against a classic
Hive-free SparkSession, verifies that parquet path, and the oracle reads the same
path. Intermediate medallion datasets stay in-session (DataFrames/temp views) unless
the agent chooses otherwise; the harness does not require or read catalog tables.
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import sys
import threading
import time
import urllib.request
from typing import Any, List, Optional

# Hard bound on the force-kill teardown of a wedged in-process JVM: confirm the JVM
# process actually EXITED and its (fixed) Spark UI port was RELEASED before the next
# cell starts, so a half-dead JVM cannot contaminate the next cell's H2 REST or bind.
_FORCE_KILL_TIMEOUT_S = 20.0

from .base import (AGENT_EXEC_TIMEOUT_S, POST_EXEC_TIMEOUT_S, AgentBrain, ExecOutcome,
                   GateOutcome, LoopState, Proposal, SparkExecutor, aux_input_env)


# The most-recently-created LocalSparkExecutor. The runner runs cells SEQUENTIALLY
# (the per-cell guard joins cell N before starting cell N+1), so there is only ever
# one "active" in-process imperative executor at a time. The per-cell wall-clock guard
# uses this handle to ABANDON a wedged cell's executor (so its later teardown can never
# clobber the next cell's fresh JVM) and force-kill the in-process JVM. See
# `abandon_active_local_executor`.
_ACTIVE_LOCAL_EXECUTOR: "Optional[LocalSparkExecutor]" = None


def _run_bounded(fn, timeout_s: float, *, name: str = "op"):
    """Run `fn()` in a daemon thread and join up to `timeout_s`.

    Returns (ok, value): ok=False if the call did not finish within the bound (the
    in-process JVM is wedged and the thread is left to die when the JVM is killed or
    the process exits). If `fn` raised, the exception is re-raised in the caller so
    existing error handling (e.g. OUTPUT_PATH_NOT_FOUND) is preserved. This is the
    bound that stops a post-execution py4j call from hanging the whole sweep.
    """
    box: dict = {"v": None, "err": None}

    def _t() -> None:
        try:
            box["v"] = fn()
        except BaseException as e:  # noqa: BLE001 -- propagated to the caller below
            box["err"] = e

    th = threading.Thread(target=_t, name=f"bounded-{name}", daemon=True)
    th.start()
    th.join(timeout_s)
    if th.is_alive():
        return False, None
    if box["err"] is not None:
        raise box["err"]
    return True, box["v"]


def _kill_proc_tree(proc, timeout_s: float = _FORCE_KILL_TIMEOUT_S) -> None:
    """HARD-kill a subprocess and CONFIRM it exited (reap it). Kills by PROCESS GROUP
    when the child has its OWN group (PySpark launches the gateway JVM in its own
    session/group), so any JVM-spawned children die too -- but NEVER kills our own
    group (that would take down the runner/test process). Escalates and waits."""
    pid = getattr(proc, "pid", None)
    if pid is None:
        return
    # Process-group kill, but only if the child is in a DISTINCT group from ours.
    if hasattr(os, "killpg") and hasattr(os, "getpgid"):
        try:
            pgid = os.getpgid(pid)
            if pgid != os.getpgid(0):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass
    try:
        proc.kill()
    except Exception:  # noqa: BLE001
        pass
    # Reap + CONFIRM exit; escalate once more if it somehow survived.
    try:
        proc.wait(timeout=timeout_s)
    except Exception:  # noqa: BLE001 -- TimeoutExpired or already-reaped
        try:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass
        try:
            proc.wait(timeout=timeout_s)
        except Exception:  # noqa: BLE001
            pass


def _wait_port_released(port: int, timeout_s: float = _FORCE_KILL_TIMEOUT_S) -> bool:
    """Block (bounded) until nothing is listening on localhost:`port`. Returns True if
    the port is confirmed free, False if it was still held at the deadline. Used after
    killing a wedged JVM so the NEXT cell -- which reuses the SAME fixed UI port -- does
    not read a half-dead JVM's REST or fail to bind."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        try:
            rc = s.connect_ex(("127.0.0.1", int(port)))
        except Exception:  # noqa: BLE001
            rc = 1
        finally:
            s.close()
        if rc != 0:  # connection refused -> nothing listening -> released
            return True
        time.sleep(0.2)
    return False


def _force_kill_local_jvm(ui_port: Optional[int] = None,
                          timeout_s: float = _FORCE_KILL_TIMEOUT_S) -> bool:
    """HARD-kill the in-process classic Spark JVM (the py4j gateway subprocess) and
    RESET PySpark's process-global singletons, so a wedged JVM is abandoned and the
    NEXT local cell launches a fresh one. Unlike a bare proc.kill(), this:
      * kills by process group (where the child has its own group) and `proc.wait()`s
        to CONFIRM the JVM exited (escalating if needed), and
      * blocks (bounded) until the fixed Spark UI `ui_port` is actually RELEASED,
    BEFORE returning -- so the next cell cannot reuse a port a half-dead JVM still
    holds or read the wrong/absent H2 REST app. Safe to call when no JVM exists.

    Returns True if the teardown is confirmed clean (JVM reaped and, when a ui_port was
    given, the port released); False if something could not be confirmed in time."""
    ok = True
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
    except Exception:  # noqa: BLE001 -- pyspark absent / import wedged: nothing to kill
        return True
    sc = None
    try:
        sc = SparkContext._active_spark_context
    except Exception:  # noqa: BLE001
        sc = None
    # Collect the JVM subprocess(es) behind the py4j gateway (instance- and class-level).
    procs: list = []
    for gw_owner in (sc, SparkContext):
        try:
            gw = getattr(gw_owner, "_gateway", None)
            proc = getattr(gw, "proc", None) if gw is not None else None
            if proc is not None and proc not in procs:
                procs.append(proc)
        except Exception:  # noqa: BLE001
            pass
    for proc in procs:
        _kill_proc_tree(proc, timeout_s)
        if getattr(proc, "returncode", "x") is None:  # still running after wait -> not clean
            ok = False
    # Reset the global singletons so a fresh JVM is launched on the next getOrCreate.
    for attr in ("_active_spark_context", "_gateway", "_jvm"):
        try:
            setattr(SparkContext, attr, None)
        except Exception:  # noqa: BLE001
            pass
    for attr in ("_instantiatedSession", "_activeSession"):
        try:
            setattr(SparkSession, attr, None)
        except Exception:  # noqa: BLE001
            pass
    # Confirm the fixed UI port is actually free before the next cell can claim it.
    if ui_port is not None:
        if not _wait_port_released(int(ui_port), timeout_s):
            ok = False
    return ok


def abandon_active_local_executor() -> bool:
    """Neutralize and force-kill the active in-process imperative executor.

    Called by the runner's per-cell wall-clock guard when a cell breaches its hard
    deadline. Marks the active executor ABANDONED (so its own later stop()/interrupt,
    fired by the dying worker thread, becomes a no-op and cannot tear down the NEXT
    cell's JVM via PySpark's class-global side effects), force-kills the JVM, and BLOCKS
    until the JVM exited and its UI port was released. Returns True if there was an
    active executor to abandon."""
    ex = _ACTIVE_LOCAL_EXECUTOR
    ui_port = getattr(ex, "ui_port", None) if ex is not None else None
    if ex is not None:
        ex._abandoned = True
    _force_kill_local_jvm(ui_port=ui_port)
    return ex is not None


class ScriptedBrain(AgentBrain):
    """Replays a fixed list of (code, command) proposals. Fully deterministic."""

    def __init__(self, proposals: List[dict]):
        self.name = "scripted"
        self._proposals = proposals
        self._i = 0

    def propose(self, state: LoopState, arm: Any) -> Proposal:
        rec = self._proposals[min(self._i, len(self._proposals) - 1)]
        self._i += 1
        return Proposal(iteration=len(state.history), code=rec["code"],
                        command=rec.get("command", "python"),
                        rationale=rec.get("rationale", "scripted"))


class LocalSparkExecutor(SparkExecutor):
    def __init__(self, out_table: str, warehouse_dir: str, ui_port: int = 4040,
                 exec_timeout_s: float = AGENT_EXEC_TIMEOUT_S,
                 post_exec_timeout_s: float = POST_EXEC_TIMEOUT_S):
        self.name = "local_spark"
        # `out_table` remains the logical contract name for provenance/symmetry with
        # SDP, but imperative LOCAL completion/oracle use AGENT_OUTPUT_PATH parquet.
        self.out_table = out_table
        self.warehouse_dir = warehouse_dir
        self.ui_port = ui_port
        self.exec_timeout_s = exec_timeout_s
        # Hard bound on each post-execution in-process Spark/py4j step (read-back,
        # interrupt, stop) so a wedged JVM cannot hang the cell after the agent program
        # has returned (the live calibration hang).
        self.post_exec_timeout_s = post_exec_timeout_s
        self._exec_token = None
        self._spark = None
        # The ACTUAL Spark UI port the session bound to (discovered from uiWebUrl).
        # Spark falls back to ui_port+1, +2, ... if the requested port is taken (e.g. a
        # previous cell's JVM is still releasing it), so H2's executor-seconds REST must
        # read the REAL bound port, not the assumed `ui_port`. None until a session exists.
        self._actual_ui_port: Optional[int] = None
        # Set True by the per-cell guard when this cell is abandoned at the hard
        # wall-clock deadline. An abandoned executor's interrupt/stop are no-ops AND it
        # refuses to (re)create a session, so the dying worker thread cannot spin up or
        # tear down a JVM that would collide with the next cell.
        self._abandoned = False
        global _ACTIVE_LOCAL_EXECUTOR
        _ACTIVE_LOCAL_EXECUTOR = self

    @property
    def spark(self):
        if self._abandoned:
            # An abandoned executor must NEVER (re)create a JVM: a dying worker thread
            # that touched .spark after the per-cell guard moved on would spin up a new
            # JVM on the SAME fixed UI port the next cell is using -> contamination.
            # This check runs FIRST, before any deadness probe, so an abandoned executor
            # never even inspects (let alone revives) the session.
            raise RuntimeError(
                "LocalSparkExecutor was abandoned by the per-cell guard; refusing to "
                "(re)create a Spark session in a dead cell.")
        # READ-BACK DECOUPLING: the agent's program may call `spark.stop()` internally
        # -- idiomatic in real pyspark scripts and shared with THIS executor's session,
        # since the agent's `getOrCreate()` returns our active session. That tears down
        # the underlying SparkContext but leaves our cached handle pointing at a DEAD
        # session. The harness output read-back/grading must NOT depend on the agent's
        # session lifecycle, so drop the dead handle here and let the builder below spin
        # up a FRESH, independent session this executor owns. Both reads that run AFTER
        # the agent program route through this property -- the run_execute completion
        # check (`read_output_path`) and `_build_profile`'s oracle reads -- so reviving
        # here fixes the read-back from disk for both. This composes cleanly with the
        # per-cell JVM teardown: agent runs -> agent may stop() -> harness revives and
        # reads the output parquet from disk -> per-cell `stop()` recycles the JVM.
        if self._spark is not None and _classic_session_is_dead(self._spark):
            self._spark = None
        if self._spark is None:
            from pyspark.sql import SparkSession
            # DEFENSIVE GUARD (belt-and-suspenders with the always-force-reset stop()):
            # if a stale classic SparkContext is STILL active (a prior cell that did not
            # tear down cleanly), getOrCreate() below would raise "Only one SparkContext
            # should be running in this JVM". Reap it FIRST -- reset the process-global
            # singletons + free the UI port -- so this cell launches a fresh JVM instead
            # of crashing. (We do NOT set spark.driver.allowMultipleContexts=true: that
            # masks the lifecycle bug and risks worse cross-cell contamination.)
            stale_ctx = False
            try:
                from pyspark import SparkContext
                stale_ctx = getattr(SparkContext, "_active_spark_context", None) is not None
            except Exception:  # noqa: BLE001 -- pyspark import probe wedged: nothing to reap
                stale_ctx = False
            # Reap OUTSIDE the import-probe try so a force-kill FAILURE is NOT swallowed:
            # proceeding into getOrCreate() blind would re-raise the very "Only one
            # SparkContext" crash this guard exists to prevent. Surface a non-clean reap.
            if stale_ctx and not _force_kill_local_jvm(ui_port=self.ui_port):
                print("[LocalSparkExecutor] WARNING: reap of a stale classic "
                      "SparkContext did not confirm clean teardown before getOrCreate()",
                      file=sys.stderr)
            # SparkSession configs such as `spark.sql.catalogImplementation` are
            # static. If a previous in-process classic session is still active,
            # builder configs below would be ignored and a Hive-backed session could
            # leak into the imperative path. Stop it first so this executor owns a
            # clean Hive-free session.
            try:
                active = SparkSession.getActiveSession()
                # Type-aware: only stop a pre-existing classic SparkSession. A Spark
                # Connect session can be long-lived controller state; stopping it here
                # tears down the wrong client and was flagged as a side effect.
                if active is not None and not _is_remote_spark_session(active):
                    active.stop()
            except Exception:  # noqa: BLE001
                pass
            self._spark = (
                SparkSession.builder.master("local[2]").appName("safe_agent_local")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", str(self.ui_port))
                .config("spark.sql.warehouse.dir", self.warehouse_dir)
                # Path-based output makes catalog implementation irrelevant for the
                # imperative contract. Keep the session explicitly in-memory and never
                # enable Hive support so save/read-by-path cannot touch ObjectStore/Derby.
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("ERROR")
            # Record the ACTUAL bound UI port (Spark may have fallen back past the
            # requested one) so H2's executor-seconds REST reads the right app.
            self._actual_ui_port = _discover_ui_port(self._spark) or self.ui_port
        return self._spark

    def reachable(self) -> bool:
        try:
            _ = self.spark
            return True
        except Exception:
            return False

    def read_table(self, name: str):
        """Compatibility for non-local/legacy callers; imperative LOCAL grading should
        use `read_output_path` so no catalog lookup is needed."""
        return self.spark.table(name)

    def read_output_path(self, path: str):
        # Reads the materialized parquet from DISK. `self.spark` revives a session the
        # executor owns if the agent stopped the shared one (see the `spark` property),
        # so this read-back never fails just because the agent called `spark.stop()`.
        return self.spark.read.parquet(path)

    def _interrupt_spark(self) -> None:
        sp = self._spark
        if sp is None or self._abandoned:
            # Abandoned: the JVM was already force-killed by the per-cell guard;
            # touching its py4j gateway now would block and could clobber the next
            # cell's session, so do nothing.
            return

        def _do() -> None:
            try:
                for q in list(sp.streams.active):
                    try:
                        q.stop()
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass
            try:
                sp.sparkContext.cancelAllJobs()
            except Exception:  # noqa: BLE001
                pass

        # Bound the interrupt itself: cancelAllJobs / query.stop are py4j calls that
        # block forever if the JVM is wedged. If they don't return in time, force-kill
        # AND reset so the next iteration gets a FRESH session, not the wedged one.
        ok, _ = _run_bounded(_do, self.post_exec_timeout_s, name="interrupt")
        if not ok:
            self._force_kill_and_reset()

    def _force_kill_and_reset(self) -> None:
        """Hard-kill the wedged in-process JVM (confirming exit + UI-port release) and
        drop the cached session so the NEXT access to `self.spark` builds a FRESH one.
        Used when an in-process step (interrupt, post-exec read-back) finds the JVM
        wedged, so subsequent iterations of the SAME cell never reuse a known-dead
        session -- the fix does not depend on the outer per-cell guard to free it."""
        _force_kill_local_jvm(ui_port=self._actual_ui_port or self.ui_port)
        self._spark = None
        self._actual_ui_port = None
        self._exec_token = None

    def _run_agent_program(self, state: LoopState, analyze_only: bool):
        """Exec `pipeline.py` as `__main__`, sharing this executor's SparkSession,
        under a hard timeout. The final output contract is path-based parquet:
        AGENT_INPUT_PATH + AGENT_OUTPUT_PATH are injected; AGENT_OUTPUT_TABLE is
        deliberately absent so stale table-writing code fails fast instead of
        initializing a metastore via saveAsTable."""
        import io

        path = os.path.join(state.workspace, "pipeline.py")
        if not os.path.exists(path):
            return 1, "[NO_CODE_PRODUCED] agent wrote no pipeline.py (empty proposal)"
        with open(path) as f:
            src = f.read()
        _ = self.spark
        out_path = state.imperative_output_path
        # Multi-input tasks: the agent's program also gets its declared AUX inputs as
        # AGENT_AUX_INPUTS (+ per-name vars). Empty for single-input tasks, so their
        # injected env is byte-for-byte unchanged. Included in the save/restore set so
        # no aux var leaks into a later cell run in this process.
        aux_env = aux_input_env(state)
        # LOCAL IMPERATIVE D6: a SEPARATE dedup table is materialized to its OWN parquet
        # path (sibling of out_path) so its grade survives the agent's `spark.stop()`,
        # mirroring AGENT_OUTPUT_PATH. Empty for tasks without a separate dedup table,
        # so their injected env is byte-for-byte unchanged (the key is then popped).
        dedup_path = getattr(state, "dedup_path", "")
        old_argv = sys.argv
        old_out, old_err = sys.stdout, sys.stderr
        old_env = {k: os.environ.get(k) for k in (
            "AGENT_INPUT_PATH", "AGENT_OUTPUT_PATH", "AGENT_OUTPUT_TABLE",
            "AGENT_DEDUP_PATH", *aux_env)}
        buf = io.StringIO()
        box: dict = {"rc": None, "log": ""}
        token = object()
        self._exec_token = token

        def _restore() -> None:
            if self._exec_token is not token:
                return
            if sys.stdout is buf:
                sys.stdout = old_out
            if sys.stderr is buf:
                sys.stderr = old_err
            sys.argv = old_argv
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

        def _worker() -> None:
            sys.argv = [path] + (["--analyze-only"] if analyze_only else [])
            os.environ["AGENT_INPUT_PATH"] = state.dataset_path
            os.environ["AGENT_OUTPUT_PATH"] = out_path
            os.environ.pop("AGENT_OUTPUT_TABLE", None)
            for _k, _v in aux_env.items():
                os.environ[_k] = _v
            if dedup_path:
                os.environ["AGENT_DEDUP_PATH"] = dedup_path
            else:
                os.environ.pop("AGENT_DEDUP_PATH", None)
            sys.stdout = buf
            sys.stderr = buf
            g = {"__name__": "__main__", "__file__": path}
            try:
                exec(compile(src, path, "exec"), g)  # noqa: S102 -- agent-owned program
                box["rc"], box["log"] = 0, buf.getvalue()
            except SystemExit as e:
                code = e.code if isinstance(e.code, int) else (1 if e.code else 0)
                box["rc"], box["log"] = code, buf.getvalue()
            except BaseException as e:  # noqa: BLE001 -- agent program owns failures
                box["rc"], box["log"] = 1, f"{buf.getvalue()}\n{type(e).__name__}: {e}"
            finally:
                _restore()

        t = threading.Thread(target=_worker, name="agent-exec", daemon=True)
        t.start()
        t.join(self.exec_timeout_s)
        if t.is_alive():
            self._interrupt_spark()
            self._exec_token = None
            if sys.stdout is buf:
                sys.stdout = old_out
            if sys.stderr is buf:
                sys.stderr = old_err
            sys.argv = old_argv
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            t.join(min(30.0, self.exec_timeout_s))
            return 124, (f"[EXECUTION_TIMEOUT] agent program exceeded "
                         f"{self.exec_timeout_s:.0f}s and was terminated (active "
                         f"streaming queries stopped, jobs cancelled).")
        return box["rc"], box["log"]

    def run_gate(self, proposal: Proposal, arm: Any, state: LoopState) -> GateOutcome:
        t0 = time.time()
        rc, log = self._run_agent_program(state, analyze_only=True)
        if rc == 0:
            return GateOutcome(failed=False, wall_s=time.time() - t0, log=log or "gate: analyzed OK")
        return GateOutcome(failed=True, wall_s=time.time() - t0,
                           error_class=_error_class_from_log(log), log=f"gate: {log}")

    def _clear_parquet_target(self, path: str) -> Optional[ExecOutcome]:
        """Remove a stale parquet output directory BEFORE execute, so a file left by a
        PRIOR iteration can never be graded as if the CURRENT iteration produced it.
        Returns an error ExecOutcome if the path exists but is not a directory (a plain
        file there is a contract violation), else None. Shared by the primary GOLD
        output and the D6 secondary dedup table so both get identical pre-execute
        cleanup -- a non-writing iteration then surfaces the real incomplete-output
        failure (OUTPUT_PATH_NOT_FOUND / REQUIRED_OUTPUT_TABLE_NOT_FOUND), not a
        stale-clean pass."""
        if os.path.exists(path):
            if not os.path.isdir(path):
                return ExecOutcome(failed=True, completed=False, wall_s=0.0,
                                   executor_seconds=None,
                                   error_class="OUTPUT_PATH_NOT_DIRECTORY",
                                   log=(f"execute: output path {path!r} exists "
                                        "but is not a directory"))
            shutil.rmtree(path)
        return None

    def run_execute(self, proposal: Proposal, arm: Any, state: LoopState) -> ExecOutcome:
        t0 = time.time()
        out_path = state.imperative_output_path
        # Output-path precheck FIRST, before any Spark session is created (a stray file
        # at the path must fail fast without spinning up a JVM). _clear_parquet_target
        # returns an OUTPUT_PATH_NOT_DIRECTORY error if a plain file sits there, else
        # rmtree's the stale parquet dir so a prior iteration's output can't be graded.
        err = self._clear_parquet_target(out_path)
        if err is not None:
            return err
        # Clear the D6 secondary dedup parquet too (when this task pins one), with the
        # SAME idiom as the primary output above. Otherwise a valid parquet from a prior
        # iteration could be graded when the current iteration fails to (re)write it.
        dedup_path = getattr(state, "dedup_path", "")
        if dedup_path:
            err = self._clear_parquet_target(dedup_path)
            if err is not None:
                return err
        # Create the session (and DISCOVER its actual UI port) BEFORE the baseline
        # snapshot, so `before` and `after` read the SAME (this executor's) app. Taken
        # before session creation, `before` would fall back to the configured ui_port and
        # could read a FOREIGN Spark UI -> a wrong/negative delta (H2 undercount on the
        # first iteration). Only when there is actually a program to run: a missing
        # pipeline.py must stay a NO_CODE failure that never spins up a JVM.
        before = None
        if os.path.exists(os.path.join(state.workspace, "pipeline.py")):
            try:
                _ = self.spark
            except Exception:  # noqa: BLE001 -- build failure flows to the agent program
                pass
            before = self._executor_seconds_snapshot()
        rc, log = self._run_agent_program(state, analyze_only=False)
        wall = time.time() - t0
        if rc != 0:
            return ExecOutcome(failed=True, completed=False, wall_s=wall,
                               executor_seconds=None, error_class=_error_class_from_log(log),
                               log=f"execute: {log}")
        after = self._executor_seconds_snapshot()
        exec_s = None
        if before is not None and after is not None:
            exec_s = max(0.0, after - before)
        # The agent program returned rc=0 and wrote _SUCCESS, but the read-back below is
        # a fresh py4j job: if the in-process JVM wedged AFTER materializing output (the
        # live calibration hang), an unbounded .collect() stalls forever -- OUTSIDE the
        # agent-exec timeout. Bound it so a wedge fails fast as EXECUTION_TIMEOUT.
        try:
            ok, _ = _run_bounded(
                lambda: self.read_output_path(out_path).limit(0).collect(),
                self.post_exec_timeout_s, name="readback")
        except Exception as e:  # noqa: BLE001
            return ExecOutcome(failed=True, completed=False, wall_s=wall,
                               executor_seconds=exec_s, error_class="OUTPUT_PATH_NOT_FOUND",
                               log=f"{log}\n[completion-check] output path "
                                   f"{out_path!r} not readable: {type(e).__name__}: {e}")
        if not ok:
            # The in-process JVM is WEDGED. Force-kill + reset NOW so the next iteration
            # of this cell rebuilds a FRESH session instead of reusing the dead one;
            # don't leave it for the outer guard / final stop().
            self._force_kill_and_reset()
            return ExecOutcome(
                failed=True, completed=False, wall_s=wall, executor_seconds=exec_s,
                error_class="EXECUTION_TIMEOUT",
                log=f"{log}\n[completion-check] read-back of output path {out_path!r} "
                    f"exceeded {self.post_exec_timeout_s:.0f}s (in-process Spark wedged "
                    f"after materializing output); JVM force-killed + reset, cell failed "
                    f"fast instead of hanging.")
        return ExecOutcome(failed=False, completed=True, wall_s=wall,
                           executor_seconds=exec_s, log=log,
                           output_tables=[out_path])

    def _executor_seconds_snapshot(self) -> Optional[float]:
        try:
            # Read the ACTUAL bound UI port (Spark may have fallen back past `ui_port`
            # when a previous JVM was still releasing it) so H2 reads the right app.
            base = f"http://localhost:{self._actual_ui_port or self.ui_port}"
            apps = _get_json(f"{base}/api/v1/applications")
            if not apps:
                return None
            app_id = apps[0]["id"]
            execs = _get_json(f"{base}/api/v1/applications/{app_id}/executors")
            return sum(float(e.get("totalDuration", 0)) for e in execs) / 1000.0
        except Exception:
            return None

    def stop(self):
        sp = self._spark
        self._spark = None
        if sp is None:
            return
        if self._abandoned:
            # The per-cell guard already force-killed the JVM. Calling sp.stop() now
            # would block on the dead py4j gateway AND (via PySpark's class-global
            # SparkContext.stop side effects) could tear down the NEXT cell's fresh
            # session, so skip it entirely.
            return
        # SparkSession.stop() is a blocking py4j call: bound it so a wedged JVM cannot
        # hang the cell at teardown.
        port = self._actual_ui_port or self.ui_port
        # ALWAYS confirm cleanup -- on the CLEAN, the TIMED-OUT, AND the RAISED-failure
        # path (try/finally), not only on timeout. ROOT CAUSE of the consecutive-A2
        # harness_error: a CLEAN sp.stop() can leave PySpark's process-global singletons
        # alive (SparkContext._active_spark_context, SparkSession._instantiatedSession/
        # _activeSession), so the NEXT imperative cell's getOrCreate() raises "Only one
        # SparkContext should be running in this JVM" and the cell crashes instantly
        # (iterations=0). _run_bounded RE-RAISES a Py4J/Spark error from sp.stop(), so the
        # reap MUST be in a finally -- otherwise a raised stop() would skip it and leak the
        # singletons. _force_kill_local_jvm reaps any surviving gateway JVM, resets those
        # singletons, and waits for the UI port to release; it is a no-op-safe reap when
        # the clean stop already tore everything down.
        try:
            _run_bounded(sp.stop, self.post_exec_timeout_s, name="stop")
        except Exception:  # noqa: BLE001
            # _run_bounded RE-RAISES a Py4J/Spark error from sp.stop(). At teardown that
            # error is MOOT -- we force-kill+reset below regardless -- and letting it
            # escape would be worse: run_cell calls stop() in a bare `finally`, so a
            # raised teardown would turn an otherwise-SUCCESSFUL cell into a harness_error.
            # (KeyboardInterrupt/SystemExit are BaseException and still propagate.)
            pass
        finally:
            # Two complementary teardowns (both per-cell isolation fixes):
            #  * _force_kill_local_jvm (#42): the HARD reap -- kill the JVM subprocess by
            #    process group, reset ALL process-global singletons, and BLOCK until the
            #    fixed UI port is released, so a CLEAN-but-leaky stop() can't hand the next
            #    cell a stale SparkContext ("Only one SparkContext...") or a held UI port.
            #  * _teardown_local_jvm (SPARK-2243): the py4j GATEWAY shutdown, GUARDED so it
            #    never severs an in-process Connect client -- recycles the gateway so the
            #    next getOrCreate() launches a genuinely fresh JVM. Idempotent/best-effort,
            #    so running it after the hard reap is harmless belt-and-suspenders.
            self._actual_ui_port = None
            _force_kill_local_jvm(ui_port=port)
            _teardown_local_jvm()




def _discover_ui_port(spark: Any) -> Optional[int]:
    """The ACTUAL Spark UI port the session bound to, parsed from
    `sparkContext.uiWebUrl` (e.g. 'http://host:4055' -> 4055). Returns None if the UI
    is disabled or the URL can't be parsed. Lets H2 read the right REST app even when
    Spark fell back past the requested port due to contention."""
    try:
        url = spark.sparkContext.uiWebUrl
    except Exception:  # noqa: BLE001
        return None
    if not url:
        return None
    try:
        return int(str(url).rsplit(":", 1)[1].split("/")[0])
    except Exception:  # noqa: BLE001
        return None


def _teardown_local_jvm() -> None:
    """SPARK-2243 per-cell JVM recycle: shut down the process-global classic py4j
    gateway so the next `SparkSession.builder.getOrCreate()` launches a brand-new JVM
    subprocess instead of re-attaching to the previous cell's JVM.

    `SparkContext.stop()` only stops the context; the gateway (and the JVM it owns)
    survive as class-level singletons, and ANY static JVM state set during the prior
    cell -- catalogImplementation, Hive/ObjectStore singletons, system properties --
    would leak forward. Clearing the gateway + the cached SparkSession singletons, and
    dropping the gateway-reuse env vars, forces a clean relaunch.

    Classic-only, two independent reasons it cannot harm the SDP/Connect path:
      1. Architecture (verified): under `--backend local` the runner process NEVER
         holds a Connect session. The local Connect server runs as a SEPARATE
         `spark-submit` subprocess (its own JVM/process group), the SDP gate/run shell
         the CLI as subprocesses, and `LocalConnectExecutor.spark`/`read_table` RAISE
         to forbid an in-process Connect session (harness/connect_helper.py, Option C).
         A Spark Connect CLIENT is gRPC -- it uses neither `SparkContext._gateway` nor
         PYSPARK_GATEWAY_*. So the only in-process JVM/gateway this clears is the
         classic imperative one we own.
      2. Defensive SCOPE (belt-and-suspenders): if -- against the architecture -- a
         Connect session is somehow live in THIS process, we BAIL OUT and clear
         nothing, so a Connect client can never be severed by this teardown.

    Fully best-effort -- never raises (called from `stop()`, which the runner invokes
    in a `finally`)."""
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
    except Exception:  # noqa: BLE001 -- pyspark not importable: nothing to tear down
        return
    # SCOPE GUARD: never touch shared py4j/session/env state while a Connect session is
    # alive in-process -- the classic gateway is not ours to recycle then.
    if _active_connect_session_present(SparkSession):
        return
    gw = getattr(SparkContext, "_gateway", None)
    if gw is not None:
        try:
            gw.shutdown()
        except Exception:  # noqa: BLE001
            pass
    # Drop the singletons so the next getOrCreate cannot re-attach to the dead JVM.
    try:
        SparkContext._gateway = None
        SparkContext._jvm = None
        SparkContext._active_spark_context = None
    except Exception:  # noqa: BLE001
        pass
    try:
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
    except Exception:  # noqa: BLE001
        pass
    # These env vars make py4j CONNECT to an existing gateway rather than launch a
    # fresh JVM; clear them so the next session truly spawns its own subprocess.
    for _k in ("PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET"):
        os.environ.pop(_k, None)


def _active_connect_session_present(spark_session_cls: Any) -> bool:
    """True iff a Spark CONNECT session is live in THIS process -- the signal that
    `_teardown_local_jvm` must NOT recycle the gateway (Connect's classic-vs-Connect
    mode is process-global, and clearing PYSPARK_GATEWAY_*/the session singletons could
    disturb the Connect client).

    In Part-1 this is ALWAYS False (the runner never creates an in-process Connect
    session -- see _teardown_local_jvm docstring), so the guard is a safety net rather
    than a hot path. Reuses `_is_remote_spark_session` -- the SAME conservative Connect
    detector the `spark` builder already trusts -- and inspects the active session plus
    the cached session singletons. Best-effort: any probe error -> treat as 'not a
    Connect session' so a classic-only teardown still proceeds."""
    candidates = []
    getter = getattr(spark_session_cls, "getActiveSession", None)
    if callable(getter):
        try:
            candidates.append(getter())
        except Exception:  # noqa: BLE001
            pass
    for attr in ("_activeSession", "_instantiatedSession"):
        candidates.append(getattr(spark_session_cls, attr, None))
    for sess in candidates:
        if sess is not None:
            try:
                if _is_remote_spark_session(sess):
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


def _classic_session_is_dead(session: Any) -> bool:
    """True iff `session` is a CLASSIC SparkSession whose underlying SparkContext has
    been stopped/torn down (e.g. the agent program called `spark.stop()`), so the
    executor must rebuild a fresh session before reading the output back.

    Never treats a Spark CONNECT client as dead: a Connect session has no in-process
    SparkContext (probing `.sparkContext` would raise), and it is not ours to recycle
    -- so we leave it alone. Best-effort and conservative in BOTH directions:
      * a Connect/remote session            -> False (not ours; don't revive)
      * a probe error on a classic session  -> True  (assume torn down; rebuild)
    so a genuinely dead classic session is always replaced, while a live classic
    session (`_jsc.sc().isStopped()` is False) is kept and reused."""
    try:
        if _is_remote_spark_session(session):
            return False
    except Exception:  # noqa: BLE001
        return False
    try:
        sc = session.sparkContext
        jsc = getattr(sc, "_jsc", None)
        if jsc is None:                       # SparkContext.stop() nulls _jsc
            return True
        return bool(jsc.sc().isStopped())     # explicit JVM-side stopped flag
    except Exception:  # noqa: BLE001 -- gateway/context gone => treat as dead
        return True


def _is_remote_spark_session(session: Any) -> bool:
    """Best-effort Spark Connect detection without importing Connect internals.

    Be conservative for tests/mocks: only an explicit boolean `is_remote` result or
    a Connect-looking class/module marks the session remote. Arbitrary Mock attrs
    must not be treated as Connect, or classic stray-session cleanup is skipped.
    """
    flag = getattr(session, "is_remote", None)
    try:
        if callable(flag):
            val = flag()
            if isinstance(val, bool):
                return val
        elif isinstance(flag, bool):
            return flag
    except Exception:  # noqa: BLE001
        return True
    cls = session.__class__
    mod_name = f"{getattr(cls, '__module__', '')}.{getattr(cls, '__name__', '')}".lower()
    return "connect" in mod_name

def _error_class_from_log(log: str) -> str:
    import re
    m = re.search(r"\[([A-Z][A-Z0-9_.]+)\]", log or "")
    if m:
        return m.group(1)
    m2 = re.search(r"([A-Za-z_]+(?:Error|Exception))\b", log or "")
    return m2.group(1) if m2 else "ERROR"


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as r:
        import json
        return json.loads(r.read().decode())
