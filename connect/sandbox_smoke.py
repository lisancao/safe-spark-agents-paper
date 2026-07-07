#!/usr/bin/env python3
"""End-to-end smoke for the spark-omnigent sandbox under Option A (mTLS + identity pinning).

The sandbox talks PLAINTEXT to a local egress sidecar (sc://127.0.0.1:15002); the sidecar holds
the per-principal client cert and originates mTLS to the remote. The remote on-box Envoy proves the
principal from the cert SAN and a Spark interceptor REJECTS unless the request user_id == that
principal. This script exercises both halves of that contract:

  positive  : connect AS our own principal -> spark.range(5) and touch our sandbox_<principal>
              schema. Should SUCCEED.
  negative  : connect asserting a DIFFERENT user_id than our cert principal. The server interceptor
              must REJECT it with a gRPC auth status (proves the pinning is real, not cosmetic).

The negative check is STRICT about what counts as proof. It returns:
  exit 0  PASS         -> the request reached the server and was rejected with a gRPC auth status
                          (UNAUTHENTICATED / PERMISSION_DENIED). This is the only outcome that
                          actually demonstrates user_id != cert-principal is refused.
  exit 1  FAIL         -> the mismatched request was ACCEPTED (no error). Pinning is broken.
  exit 2  INCONCLUSIVE -> any transport/TLS/DNS/connection/unexpected error (e.g. server down,
                          sidecar not up, UNAVAILABLE). This proves NOTHING about pinning, so it is
                          deliberately NOT a pass.

This is the POST-DEPLOY smoke: it requires the sidecar up and a reachable Connect server. No server
is deployed yet, so locally we only `python -m py_compile` it. Run it inside the sandbox once the
server exists:

  # positive (uses the entrypoint-derived SPARK_REMOTE = sc://127.0.0.1:15002/;user_id=<principal>)
  python connect/sandbox_smoke.py --mode positive

  # negative (exit 0 == a genuine pinning rejection was OBSERVED; exit 2 == inconclusive, not a pass)
  python connect/sandbox_smoke.py --mode negative

Identity is read from the environment exactly as the entrypoint sets it, so the positive path can
never accidentally assert the wrong user_id.
"""
import argparse
import os
import sys

import grpc
from grpc import StatusCode

from pyspark.sql import SparkSession

# gRPC statuses that represent a genuine SERVER-SIDE auth/pinning rejection (the only PASS signal).
# UNAUTHENTICATED: identity not accepted. PERMISSION_DENIED: authenticated but user_id != the
# cert-verified principal (a typical interceptor verdict). Everything else is not proof of pinning.
_AUTH_REJECTION_CODES = {StatusCode.UNAUTHENTICATED, StatusCode.PERMISSION_DENIED}

# Exit codes (see module docstring): 0 PASS, 1 FAIL (accepted), 2 INCONCLUSIVE (transport/other).
EXIT_PASS, EXIT_FAIL, EXIT_INCONCLUSIVE = 0, 1, 2


def _principal() -> str:
    p = os.environ.get("AGENT_PRINCIPAL")
    if not p:
        sys.exit("AGENT_PRINCIPAL not set (the entrypoint sets it; export it to run standalone).")
    return p


def _sidecar_addr() -> str:
    return os.environ.get("SIDECAR_GRPC_ADDR", "127.0.0.1:15002")


def _remote_for(user_id: str) -> str:
    # Plaintext to the loopback sidecar (no token, no use_ssl -> insecure loopback hop); the sidecar
    # does the real mTLS. The remote's identity comes from the CERT, not from this user_id; user_id
    # is only what the interceptor cross-checks against the cert-derived principal.
    return f"sc://{_sidecar_addr()}/;user_id={user_id}"


def positive() -> int:
    principal = _principal()
    schema = os.environ.get("AGENT_SCHEMA", f"sandbox_{principal}")
    remote = _remote_for(principal)
    print(f"[positive] connecting as principal '{principal}' via {remote}")
    spark = SparkSession.builder.remote(remote).getOrCreate()

    n = spark.range(5).count()
    assert n == 5, f"expected 5 rows from spark.range(5), got {n}"
    print(f"[positive] spark.range(5).count() == {n}  (execution is remote, no local JVM)")

    # Touch our own writable namespace: create it (idempotent), write a row, read it back.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE OR REPLACE TABLE {schema}.smoke AS SELECT id FROM range(3)")
    got = spark.table(f"{schema}.smoke").count()
    assert got == 3, f"expected 3 rows in {schema}.smoke, got {got}"
    print(f"[positive] wrote+read {schema}.smoke ({got} rows) — own schema OK")

    spark.stop()
    print("[positive] PASS")
    return 0


def _grpc_status_of(exc: BaseException):
    """Best-effort extraction of a grpc.StatusCode from a pyspark/grpc exception.

    Returns a grpc.StatusCode, or None if the error carries no gRPC status (e.g. a raw
    TLS/connection error). We classify ONLY on this structured code, never on message text, so a
    server-down / DNS / TLS failure cannot masquerade as a pinning rejection.
    """
    # pyspark wraps gRPC errors in SparkConnectGrpcException, which exposes getGrpcStatusCode().
    getter = getattr(exc, "getGrpcStatusCode", None)
    if callable(getter):
        try:
            code = getter()
            if isinstance(code, StatusCode):
                # UNKNOWN is pyspark's default when no real gRPC status was attached -> not proof.
                return None if code == StatusCode.UNKNOWN else code
        except Exception:  # noqa: BLE001 — fall through to other extraction paths
            pass
    # A raw grpc.RpcError (rare here) exposes .code().
    if isinstance(exc, grpc.RpcError):
        try:
            code = exc.code()
            if isinstance(code, StatusCode):
                return code
        except Exception:  # noqa: BLE001
            pass
    # Walk the cause chain (pyspark sometimes chains the original RpcError).
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        return _grpc_status_of(cause)
    return None


def negative() -> int:
    """Assert a user_id that does NOT match our cert principal; the server must reject it.

    Exit 0 ONLY when the server returns a gRPC auth status (UNAUTHENTICATED / PERMISSION_DENIED).
    Acceptance is exit 1 (FAIL). Anything else (no gRPC status, transport/TLS/DNS error, or a
    non-auth status) is exit 2 (INCONCLUSIVE) — it does not prove pinning, so it is not a pass.
    """
    principal = _principal()
    impostor = f"{principal}_evil"
    remote = _remote_for(impostor)
    print(f"[negative] real cert principal is '{principal}', but asserting user_id='{impostor}'")
    print(f"[negative] connecting via {remote} — expecting a gRPC auth-status REJECTION")

    try:
        spark = SparkSession.builder.remote(remote).getOrCreate()
        # Force a round-trip so the interceptor actually evaluates the request.
        spark.range(1).count()
    except Exception as exc:  # noqa: BLE001 — classify strictly below; do NOT treat all as pass
        first_line = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
        code = _grpc_status_of(exc)
        if code in _AUTH_REJECTION_CODES:
            print(f"[negative] correctly REJECTED with gRPC {code.name}: {first_line}")
            print("[negative] PASS — server-side pinning refused a mismatched user_id.")
            return EXIT_PASS
        code_label = code.name if code is not None else "no gRPC status"
        print(f"[negative] INCONCLUSIVE ({code_label}): {first_line}")
        print("[negative] This is NOT a pinning rejection (server down / TLS / DNS / sidecar / other).")
        print("[negative] Proves nothing about user_id pinning — re-run against a reachable server.")
        return EXIT_INCONCLUSIVE
    else:
        print("[negative] FAIL: connection asserting a mismatched user_id was ACCEPTED.")
        print("[negative] The server-side interceptor (deploy/auth) is not pinning user_id to the cert.")
        return EXIT_FAIL


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=["positive", "negative"], default="positive")
    args = ap.parse_args()
    return positive() if args.mode == "positive" else negative()


if __name__ == "__main__":
    raise SystemExit(main())
