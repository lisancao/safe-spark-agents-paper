"""safe_agent_study harness package.

Modules:
  schema        - frozen results.jsonl row + env sidecar contract
  cost          - executor-seconds + USD accounting (H2)
  oracles       - arm-BLIND automated defect grader (reuses defect_battery/quantify.py)
  arm_manifest  - arm manifests + identical-except-loop invariant (pre-reg §3)
  runner        - multi-arm loop, cost model, blind grading, row emission
  backends/     - pluggable agent brain + Spark executor (replay | live)
"""
