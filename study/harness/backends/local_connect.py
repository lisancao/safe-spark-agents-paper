"""LOCAL Spark Connect server lifecycle + a local-FS SDP executor (Part-1 substrate).

Part 1 of the study isolates the imperative-vs-SDP PARADIGM by running each arm on
its HOME local engine, off the remote EKS Connect substrate (which is Part-2's
finding: imperative arms can't run on remote Connect at all). The two engines:

  * imperative arms (A, B2)  -> classic in-process local[*] Spark
    (`harness.backends.local.LocalSparkExecutor`), where classic Spark works so the
    imperative agents complete;
  * SDP arms (B, B1)         -> a LOCAL single-node Spark Connect server, because the
    SDP CLI (`pyspark/pipelines/cli.py`) FUNDAMENTALLY requires Spark Connect even
    locally (a bare in-process session fails with ONLY_SUPPORTED_WITH_SPARK_CONNECT).

This module owns (a) the local Connect server's lifecycle and (b) a thin
`LocalConnectExecutor` that points the existing live `ConnectExecutor` at that local
server and reads the per-seed input straight off the shared local filesystem (no S3
staging needed locally).

Startup mechanism (PROVEN; validated live, and mirrors the gitops_demo reference
`gitops_demo/local_spark_connect.sh` + `ensure_schema.py`): a pip `pyspark[connect]`
install has no `sbin/start-connect-server.sh`, but it bundles
`jars/spark-connect_*.jar` (which contains
`org.apache.spark.sql.connect.service.SparkConnectServer`) and `bin/spark-submit`
(which puts `jars/*` on the classpath). Launching that class via spark-submit with
the special `spark-internal` primary resource starts the Connect gRPC service. We
resolve SPARK_HOME from the installed pyspark (`os.path.dirname(pyspark.__file__)`).

This is the CONTROLLER surface: it legitimately holds a Spark session (to CREATE the
default schema the SDP CLI resolves flow names against). The agent never runs it.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from .base import CONNECT_CMD_TIMEOUT_S
from .live import ConnectExecutor, _spark_home

# The study dir (…/experiments/safe_agent_study), three levels up from this file
# (backends -> harness -> study). Used as the subprocess CWD so `python3 -m
# harness.connect_helper …` resolves the package without poisoning the parent.
_STUDY_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


def _run_connect_helper(argv_tail: List[str],
                        timeout_s: float = CONNECT_CMD_TIMEOUT_S) -> subprocess.CompletedProcess:
    """Run `python3 -m harness.connect_helper <argv_tail>` as a SUBPROCESS (Option C).

    The helper creates the Spark Connect session in its OWN process and exits, so the
    long-lived runner process never flips into Connect mode (which would break the
    imperative classic `LocalSparkExecutor.getOrCreate()` -- the global mode bug).

    Bounded by a hard timeout (the controller op could hang on a wedged server): on
    timeout the helper is reaped and a non-zero CompletedProcess is returned, so callers
    (ensure_schema / build_output_profile_subprocess) fail gracefully, never hang."""
    cmd = [sys.executable, "-m", "harness.connect_helper", *argv_tail]
    try:
        return subprocess.run(cmd, cwd=_STUDY_DIR, env=dict(os.environ),
                              capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return subprocess.CompletedProcess(
            cmd, 124, stdout=out,
            stderr=f"{err}\n[EXECUTION_TIMEOUT] connect helper exceeded {timeout_s:.0f}s.")


class LocalConnectServer:
    """Start/stop ONE single-node Spark Connect server for the Part-1 SDP arms.

    The proven-good launch (validated live) is::

        $SPARK_HOME/bin/spark-submit \\
          --class org.apache.spark.sql.connect.service.SparkConnectServer \\
          --conf spark.connect.grpc.binding.port=<port> \\
          --conf spark.ui.port=<ui_port> \\
          --conf spark.sql.warehouse.dir=file:///<warehouse_dir> \\
          --conf spark.sql.catalogImplementation=in-memory \\
          spark-internal

    The SDP `ConnectExecutor` dials `sc://localhost:<port>/;user_id=<user>` and, for
    H2, reads the driver UI REST at `http://localhost:<ui_port>` (the D-5 stage-diff
    method). `spark.ui.port` is pinned EXPLICITLY so the SDP H2 REST target is
    deterministic and never collides with the imperative `LocalSparkExecutor`'s own
    in-process UI (which the local backend puts on `<ui_port>+1`).
    """

    def __init__(self, port: int = 15002, ui_port: int = 4040,
                 warehouse_dir: Optional[str] = None, user_id: str = "alice",
                 wait_secs: int = 120, log_file: Optional[str] = None,
                 spark_home: Optional[str] = None):
        self.port = port
        self.ui_port = ui_port
        # local file:// warehouse; default to a tmp dir if unset
        self.warehouse_dir = os.path.abspath(
            warehouse_dir or os.path.join(os.getcwd(), ".local_connect_wh"))
        self.user_id = user_id
        self.wait_secs = wait_secs
        self.log_file = log_file
        self._spark_home = spark_home
        self._proc: Optional[subprocess.Popen] = None
        self._log_fh = None

    # -- identity the SDP executor needs ------------------------------------
    @property
    def remote(self) -> str:
        """The SPARK_REMOTE / SparkSession.remote URL the SDP CLI dials."""
        return f"sc://localhost:{self.port}/;user_id={self.user_id}"

    @property
    def rest_url(self) -> str:
        """The driver UI REST base used for the H2 stage-diff (D-5)."""
        return f"http://localhost:{self.ui_port}"

    def spark_home(self) -> str:
        return self._spark_home or os.environ.get("SPARK_HOME") or _spark_home()

    # -- the proven-good spark-submit command (pure -> unit-testable) -------
    def submit_argv(self) -> List[str]:
        sh = self.spark_home()
        return [
            os.path.join(sh, "bin", "spark-submit"),
            "--class", "org.apache.spark.sql.connect.service.SparkConnectServer",
            "--conf", f"spark.connect.grpc.binding.port={self.port}",
            "--conf", f"spark.ui.port={self.ui_port}",
            "--conf", f"spark.sql.warehouse.dir=file://{self.warehouse_dir}",
            "--conf", "spark.sql.catalogImplementation=in-memory",
            # disable artifact isolation so the single-runner server serves the CLI
            # directly (matches the gitops_demo reference launcher).
            "--conf", "spark.sql.artifact.isolation.enabled=false",
            "spark-internal",
        ]

    # -- lifecycle ----------------------------------------------------------
    def start(self, ensure_database: Optional[str] = "default",
              ensure_catalog: str = "spark_catalog") -> "LocalConnectServer":
        """Launch the server (background), block until the gRPC port accepts TCP,
        then CREATE the default schema the SDP CLI resolves into.

        Reuses an already-running server on the port (idempotent), matching the
        gitops_demo reference. Raises RuntimeError if the port never comes up.
        """
        if self._port_open(self.port):
            # something is already listening; reuse it (idempotent).
            print(f"[local-connect] port {self.port} already reachable; reusing.",
                  file=sys.stderr)
        else:
            os.makedirs(self.warehouse_dir, exist_ok=True)
            argv = self.submit_argv()
            if self.log_file:
                os.makedirs(os.path.dirname(self.log_file) or ".", exist_ok=True)
                self._log_fh = open(self.log_file, "w")
                stdout = stderr = self._log_fh
            else:
                stdout = stderr = subprocess.DEVNULL
            print(f"[local-connect] starting Spark Connect server on port "
                  f"{self.port} (ui {self.ui_port}); SPARK_HOME={self.spark_home()}",
                  file=sys.stderr)
            # start_new_session=True puts the server (and any JVM child it spawns) in its
            # OWN process group so teardown can SIGKILL the WHOLE group, not just the
            # launcher -- the process-group teardown the harness uses everywhere and that
            # the per-cell hard reset (harness_faults) relies on to free the gRPC/UI ports.
            self._proc = subprocess.Popen(argv, stdout=stdout, stderr=stderr,
                                          start_new_session=True)
            if not self._wait_for_port():
                self.stop()
                raise RuntimeError(
                    f"local Spark Connect server did not bind port {self.port} "
                    f"within {self.wait_secs}s (see log {self.log_file or '<devnull>'}).")
            print(f"[local-connect] reachable on {self.remote}", file=sys.stderr)
        if ensure_database:
            # Belt-and-suspenders: the JVM is now up, so a failure here (e.g. the
            # CREATE SCHEMA session erroring) MUST NOT leak it. Tear the server down
            # before re-raising. (The runner ALSO wraps start() in a try/finally, so
            # this is a second, local guarantee of the same invariant.)
            try:
                self.ensure_schema(ensure_catalog, ensure_database)
            except Exception:
                self.stop()
                raise
        return self

    def ensure_schema(self, catalog: str = "spark_catalog", database: str = "default") -> None:
        """`CREATE SCHEMA IF NOT EXISTS <catalog>.<database>` so the SDP CLI does not
        fail the dry-run/run with `[SCHEMA_NOT_FOUND]` (a false failure unrelated to
        any pipeline defect). Mirrors `gitops_demo/ensure_schema.py`.

        Option C: this runs in a SUBPROCESS Connect helper, NOT an in-process
        `SparkSession.builder.remote(...)`. The runner process must never create a
        Connect session (classic-vs-Connect mode is process-global; a parent Connect
        session breaks the imperative classic `LocalSparkExecutor`)."""
        proc = _run_connect_helper([
            "ensure-schema", "--remote", self.remote,
            "--catalog", catalog, "--database", database])
        if proc.returncode != 0:
            raise RuntimeError(
                f"ensure_schema subprocess failed (rc={proc.returncode}) for "
                f"`{catalog}`.`{database}` on {self.remote}:\n{proc.stdout}\n{proc.stderr}")
        print(f"[local-connect] ensured schema `{catalog}`.`{database}` (subprocess)",
              file=sys.stderr)

    def stop(self) -> None:
        """Tear down the server we started (no-op if we reused an existing one).

        Kill the WHOLE PROCESS GROUP (the launcher started with start_new_session=True),
        SIGTERM then SIGKILL, so the spark-submit launcher AND its JVM child both die and
        the gRPC/UI ports are freed -- a bare terminate() can leave the JVM holding the
        port and wedge the next run. Best-effort throughout; never raises."""
        if self._proc is not None:
            import signal
            try:
                pgid = os.getpgid(self._proc.pid)
            except Exception:  # noqa: BLE001
                pgid = None
            try:
                if pgid is not None:
                    os.killpg(pgid, signal.SIGTERM)
                else:
                    self._proc.terminate()
            except Exception:  # noqa: BLE001
                pass
            try:
                self._proc.wait(timeout=30)
            except Exception:  # noqa: BLE001
                try:
                    if pgid is not None:
                        os.killpg(pgid, signal.SIGKILL)
                    else:
                        self._proc.kill()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    self._proc.wait(timeout=10)
                except Exception:  # noqa: BLE001
                    pass
            self._proc = None
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            finally:
                self._log_fh = None

    # -- helpers ------------------------------------------------------------
    def _wait_for_port(self) -> bool:
        deadline = time.time() + self.wait_secs
        while time.time() < deadline:
            if self._port_open(self.port):
                return True
            time.sleep(2)
        return False

    @staticmethod
    def _port_open(port: int) -> bool:
        s = socket.socket()
        s.settimeout(1)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except OSError:
            return False
        finally:
            s.close()


class LocalConnectExecutor(ConnectExecutor):
    """The SDP executor for Part-1, pointed at the LOCAL Connect server.

    Identical to the live `ConnectExecutor` (same SDP CLI gate/run, same D-5
    stage-diff H2 against the driver UI) EXCEPT for input staging: a local
    single-node server shares this machine's filesystem, so there is no S3/IRSA
    round-trip — `stage_input` simply returns a `file://` URI for the local NDJSON
    (VERIFIED: opus's SDP reading `file:///<path>` dry-runs COMPLETED locally). This
    is the local analogue of the remote executor's `createDataFrame -> write.text`
    staging (D-3), and it keeps the per-seed input the agent reads byte-identical to
    the one the oracle reads for ground truth.
    """

    def __init__(self, spark_remote: str, spark_rest_url: Optional[str] = None):
        super().__init__(spark_remote, spark_rest_url, staging_base=None)
        self.name = "local_connect"

    def stage_input(self, local_path: str, subkey: Optional[str] = None) -> str:
        """Return a `file://` URI for the local NDJSON (no copy — shared FS).

        `subkey` (AUX inputs) needs no distinct destination here: aux inputs are
        already distinct local files (different generator stems), so their file://
        URIs differ naturally. The param keeps the signature symmetric with the
        remote `ConnectExecutor.stage_input` so the runner stages both the same way.
        """
        if not local_path:
            return local_path
        if "://" in local_path:
            return local_path
        return f"file://{os.path.abspath(local_path)}"

    # -- Option C: NEVER a Connect session in the parent runner process ------
    # classic-vs-Connect mode is process-global, so a parent Connect session would
    # break the imperative classic LocalSparkExecutor. The SDP gate/execute already
    # subprocess the CLI, and the H2 stage-diff reads the driver UI over HTTP, so the
    # only remaining parent need -- reading the agent's output back for the OUTPUT
    # oracle -- is delegated to a subprocess helper here. The session accessors are
    # GUARDED to fail LOUDLY so this invariant can't silently regress.
    _PARENT_SESSION_MSG = (
        "Refusing to create a Spark Connect session in the Part-1 runner process: "
        "classic-vs-Connect mode is process-global and a parent Connect session "
        "breaks the imperative classic LocalSparkExecutor (CONNECT_URL_NOT_SET). "
        "Use the subprocess helper (LocalConnectExecutor.build_output_profile_subprocess "
        "/ harness.connect_helper) instead.")

    @property
    def spark(self):
        raise RuntimeError(self._PARENT_SESSION_MSG)

    def read_table(self, name: str):
        raise RuntimeError(self._PARENT_SESSION_MSG)

    def build_output_profile_subprocess(self, task_spec: Dict[str, Any],
                                        dataset: Optional[str], out_table: str):
        """Build the SDP run's OutputProfile in a SUBPROCESS Connect helper (Option C).

        Shells to `harness.connect_helper output-profile`, which creates the Connect
        session in ITS OWN process, reads the agent's materialized `out_table` back via
        `spark.table`, runs `output_oracles.build_output_profile`, and serialises the
        profile fields to a JSON result file we reconstruct here. The runner's
        `_build_profile` calls THIS (when present) instead of touching `executor.spark`,
        so the parent never enters Connect mode. Returns an `OutputProfile`, or `None`
        if there is no contract/dataset to grade against.
        """
        from harness import oracles as oraclesmod

        contract = task_spec.get("output_contract")
        if not contract or not dataset:
            return None
        defects = task_spec.get("defects_in_scope", [])
        import json
        import tempfile
        result_path = tempfile.NamedTemporaryFile(
            prefix="ssa_profile_", suffix=".json", delete=False).name
        try:
            proc = _run_connect_helper([
                "output-profile", "--remote", self.spark_remote,
                "--input", dataset, "--contract", json.dumps(contract),
                "--defects", ",".join(defects), "--result", result_path])
            prof = oraclesmod.OutputProfile()
            if proc.returncode != 0 or not os.path.getsize(result_path):
                # the run COMPLETED but we could not read its output back; record the
                # failure on the profile (mirrors build_output_profile's read-error
                # path) rather than crash the sweep.
                prof.extra["output_profile_subprocess_error"] = (
                    f"rc={proc.returncode}: {proc.stderr.strip()[-2000:]}")
                return prof
            with open(result_path) as f:
                payload = json.load(f)
            for k, v in payload.items():
                if k == "extra":
                    prof.extra.update(v or {})
                elif hasattr(prof, k):
                    setattr(prof, k, v)
            return prof
        finally:
            try:
                os.unlink(result_path)
            except OSError:
                pass
