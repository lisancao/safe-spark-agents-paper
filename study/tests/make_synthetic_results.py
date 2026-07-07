"""Generate a SYNTHETIC results.jsonl to validate the analysis layer plumbing.

THIS IS NOT EXPERIMENTAL DATA. It is a deterministic, clearly-labelled fixture
whose ONLY purpose is to exercise analyze.py end-to-end (bootstrap CIs, GLMM,
Holm, H2) so we can confirm the statistics code runs and produces a coherent
headline table BEFORE any real sweep. Every row carries notes="SYNTHETIC
FIXTURE -- not real data". The silent-defect pattern is invented to give the
estimators something with variation to chew on; it must NOT be read as a result.

Usage: python tests/make_synthetic_results.py <out.jsonl>
"""
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness.cost import ExecutorConfig  # noqa: E402
from harness.schema import ResultRow  # noqa: E402

TASKS = ["orders_silver_gold", "p1_medallion", "p2_cdc", "p3_windows", "p4_fanout", "p5_mart"]
SEEDS = [42, 1337, 2718, 3141, 5772, 8675, 9001, 11235, 27182, 31415]
ARMS = ["A", "B", "B1", "B2"]

# invented silent-defect propensities per arm (NOT a result; plumbing only)
PROP = {"A": 0.55, "B": 0.10, "B1": 0.30, "B2": 0.35}
CFG = ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge")


def gen(out_path: str, seed: int = 7):
    rng = random.Random(seed)
    rows = []
    for task in TASKS:
        for s in SEEDS:
            for arm in ARMS:
                silent = rng.random() < PROP[arm]
                # ~12% of runs never reach a correct output (exercise the B9 H2
                # ITT-vs-complete-case split). A never-correct run cannot be silent.
                reached = rng.random() > 0.12
                if not reached:
                    silent = False
                completed = reached
                gate = arm in ("B", "B2")
                # synthetic loop costs: gated arms intercept some failures at $0
                fails = rng.randint(0, 3)
                intercepts = rng.randint(0, fails) if gate else 0
                exec_iters = max(1, fails - intercepts) + 1
                exec_s_per = rng.uniform(60, 120)
                exec_to_correct = exec_s_per * exec_iters
                total_exec_s = exec_to_correct
                usd = total_exec_s / 3600.0 * CFG.price_usd_per_executor_hour
                defect_classes = ["D8"] if silent else []
                row = ResultRow(
                    run_id=f"{task}__{arm}__seed{s}",
                    task=task, arm=arm, seed=s,
                    spark_version="4.1.0.dev4", image_digest="sha256:SYNTHETIC",
                    git_sha="SYNTHETIC", base_model_id="claude-sonnet-4-6",
                    executor_config=CFG.to_dict(),
                    silent_defect=silent, defect_classes=defect_classes,
                    detection_stage="never" if silent else ("dry_run" if gate and fails else "n/a"),
                    iterations=exec_iters + intercepts,
                    wall_s=total_exec_s / CFG.instances + intercepts * 8.0,
                    executor_seconds=total_exec_s, usd=usd,
                    exit_class="completed" if reached else "max_iterations",
                    task_success=completed, reached_correct=reached,
                    iterations_to_green=(exec_iters + intercepts) if reached else None,
                    wall_s_to_green=total_exec_s / CFG.instances + intercepts * 8.0,
                    executor_seconds_to_correct=exec_to_correct,
                    dry_run_intercepts=intercepts, failing_iterations=fails,
                    per_defect_detection={"D8": "never" if silent else "n/a"},
                    backend="synthetic", notes="SYNTHETIC FIXTURE -- not real data",
                    timestamp_utc="2026-06-23T00:00:00Z",
                )
                rows.append(row)
    with open(out_path, "w") as f:
        for r in rows:
            f.write(r.to_json() + "\n")
    return len(rows)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "synthetic_results.jsonl"
    n = gen(out)
    print(f"wrote {n} SYNTHETIC rows to {out}")
