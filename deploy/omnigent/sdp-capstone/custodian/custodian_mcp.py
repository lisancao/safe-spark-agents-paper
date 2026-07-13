#!/usr/bin/env python3
"""SP4 Stage 2: the fleet CUSTODIAN as a native Omnigent MCP server (stdio).

Omnigent spawns this process and calls its tools; the PSK + per-tenant tokens live ONLY here, so a
sub-agent that calls submit_pipeline never sees a credential (custody boundary = a real process
boundary). Self-bootstraps connectivity to the live Section 3 tenants (resolves the Connect PSK from
the cluster + opens per-tenant port-forwards). Runs in system python3 (has both `mcp` and pyspark)."""
import os, sys, base64, subprocess, atexit, time

SC = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SC)  # find custodian_capstone + seed_raw regardless of cwd

def _bootstrap():
    # resolve the Connect PSK from the cluster (held only in this process) BEFORE importing the custodian
    psk_b64 = subprocess.check_output(
        ["kubectl", "-n", "spark", "get", "secret", "spark-connect-psk", "-o", "jsonpath={.data.token}"]
    ).decode().strip()
    os.environ["SC_PSK"] = base64.b64decode(psk_b64).decode()
    # per-tenant port-forwards (custodian -> live tenant Connect servers)
    pfs = []
    for svc, port in [("spark-connect-tenant-a", 15010), ("spark-connect-tenant-b", 15011), ("spark-connect-tenant-c", 15012)]:
        pfs.append(subprocess.Popen(
            ["kubectl", "-n", "spark", "port-forward", f"svc/{svc}", f"{port}:15002"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    atexit.register(lambda: [p.terminate() for p in pfs])
    time.sleep(6)  # let the forwards establish
    print(f"[custodian-mcp] bootstrapped: PSK resolved, 3 tenant port-forwards up", file=sys.stderr, flush=True)

_bootstrap()
import custodian_capstone as cust   # reads SC_PSK at import; bootstrap set it above
import seed_raw
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("sdp-custodian")

@mcp.tool()
def seed_raw_data(customer: str) -> dict:
    """Seed the messy raw source data for a customer into its OWN tenant, so its medallion has an input.
    customer is one of: retail (orders), saas (cdc), payments (payments+fx). Returns the seeded row counts."""
    return seed_raw.seed(customer)

@mcp.tool()
def submit_pipeline(customer: str, code: str) -> dict:
    """Submit a COMPLETE OSS SDP medallion pipeline (python source) for a customer. The custodian runs it
    over THAT customer's own live tenant (you never see a credential), materializes the datasets,
    enforces the customer's contextual policy, and returns {pass, materialize:{materialized,errors},
    policy:{checks,pass}}. On pass=false, read materialize.errors and policy.checks, fix, and resubmit."""
    return cust.submit(customer, code)

@mcp.tool()
def probe_isolation(customer: str, other_namespace: str) -> dict:
    """Verify per-tenant isolation: attempt to read another customer's medallion namespace
    (e.g. saas_med, pay_med, retail_med) from this customer's tenant session. Returns denied=true when
    the cross-tenant read is refused, which is the expected, correct outcome."""
    return cust.probe_isolation(customer, other_namespace)

if __name__ == "__main__":
    mcp.run()
