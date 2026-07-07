# `pipeline-definitions/` — the declarative source of truth

This directory is the **GitOps source of truth** for the demo's Spark Declarative
Pipelines (SDP). Each subdirectory is one pipeline:

```
pipeline-definitions/
  <slug>/
    spark-pipeline.yml          # the SDP spec (name, storage, catalog, database, libraries glob)
    transformations/
      pipeline.py               # the agent's @dp.table / @dp.materialized_view code
```

## How entries get here

Agents do **not** edit this directory by hand and do **not** run Spark. An agent run
(`gitops_demo/agent_pr_author.py`) authors a new `<slug>/` subdirectory and opens a
**pull request**. The PR is the agent's entire footprint — files + git + PR.

## What acts on these files

| Step | Tool | When |
|------|------|------|
| **Gate** | `spark-pipelines dry-run --spec <slug>/spark-pipeline.yml` | on the PR (CI) |
| **Reconcile** | `spark-pipelines run --spec <slug>/spark-pipeline.yml` | on merge to main (controller) |

The gate and the reconcile both hold a Spark session (`SPARK_REMOTE`). The agent that
wrote the files never did. That asymmetry is the whole point — see
[`../README.md`](../README.md).

The `.gitkeep` keeps this directory tracked while it is empty; real pipeline
subdirectories arrive via agent PRs.
