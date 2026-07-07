"""Render a declarative SDP artifact (the GitOps source of truth).

This module turns an agent's authored transform code into the two versioned files
that ARE the pipeline under GitOps:

    pipeline-definitions/<slug>/spark-pipeline.yml          (the SDP spec)
    pipeline-definitions/<slug>/transformations/pipeline.py (the agent's @dp.table code)

The spec shape matches what `pyspark/pipelines/cli.py` accepts (see
`unpack_pipeline_spec`): `name`, `storage`, `catalog`, `database`, and a
`libraries` glob over `transformations/**`. The CI dry-run gate and the merge
reconcile step both read these files VERBATIM -- the agent never runs them.

SAFETY: this module imports NO pyspark and opens NO Spark session. It only writes
text files. It is imported by `agent_pr_author.py`, which is the agent surface; the
agent surface must stay session-free (see README's safety-boundary table). YAML is
hand-rendered (no yaml dependency) so the agent path has no Spark/heavy imports.
"""
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional

# Local-demo storage root. On the live cluster this becomes `s3a://...` and is
# supplied by the controller -- never chosen by the agent (see PRODUCTION_EKS.md).
LOCAL_STORAGE_ROOT = "file:///tmp/safe-spark-agents-gitops"

# Fixed catalog/database for the demo namespace (matches the study's spark_catalog).
DEFAULT_CATALOG = "spark_catalog"
DEFAULT_DATABASE = "gitops_demo"

# The libraries glob every spec uses: the agent's transform modules live here.
LIBRARIES_GLOB = "transformations/**"

PIPELINE_DEFINITIONS_DIR = "pipeline-definitions"


def slugify(value: str) -> str:
    """Lowercase, hyphenated, filesystem/branch-safe slug."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return s or "pipeline"


def storage_uri(slug: str, storage_root: Optional[str] = None) -> str:
    """The SDP `storage:` base for this pipeline (checkpoints/metadata root)."""
    root = (storage_root or LOCAL_STORAGE_ROOT).rstrip("/")
    return f"{root}/{slug}/storage"


def render_spec(slug: str, *, name: Optional[str] = None,
                storage_root: Optional[str] = None,
                catalog: str = DEFAULT_CATALOG,
                database: str = DEFAULT_DATABASE) -> str:
    """Render `spark-pipeline.yml` text for the pipeline `slug`.

    Hand-rendered YAML (stable key order, no yaml dependency). Shape is exactly the
    fields `cli.py` allows; `libraries` is a single glob over `transformations/**`.
    """
    pipeline_name = name or f"gitops_demo__{slug}"
    return (
        f"name: {pipeline_name}\n"
        f"storage: {storage_uri(slug, storage_root)}\n"
        f"catalog: {catalog}\n"
        f"database: {database}\n"
        "libraries:\n"
        "  - glob:\n"
        f"      include: {LIBRARIES_GLOB}\n"
    )


_PIPELINE_HEADER = (
    "# Declarative SDP transform -- the GitOps source of truth for this pipeline.\n"
    "# Authored by the agent (agent_pr_author.py) and rendered by sdp_artifact.py.\n"
    "# CI runs `spark-pipelines dry-run` on this on the PR; merge runs "
    "`spark-pipelines run`.\n"
    "# The agent that wrote this file never held a Spark session.\n"
)


def render_pipeline_py(code: str) -> str:
    """Render `transformations/pipeline.py` from the agent's authored code.

    The agent's code is emitted VERBATIM under a provenance header. No harness
    SparkSession/main is injected -- under SDP the framework owns execution.
    """
    body = (code or "").rstrip() + "\n"
    return f"{_PIPELINE_HEADER}\n{body}"


def artifact_paths(slug: str, base_dir: str = "") -> Dict[str, str]:
    """The two artifact paths for `slug`, relative to `base_dir` (default: cwd-rel)."""
    root = os.path.join(base_dir, PIPELINE_DEFINITIONS_DIR, slug)
    return {
        "spec": os.path.join(root, "spark-pipeline.yml"),
        "pipeline": os.path.join(root, "transformations", "pipeline.py"),
    }


def write_artifact(base_dir: str, slug: str, code: str, *,
                   name: Optional[str] = None,
                   storage_root: Optional[str] = None,
                   catalog: str = DEFAULT_CATALOG,
                   database: str = DEFAULT_DATABASE) -> List[str]:
    """Write both artifact files under `base_dir/pipeline-definitions/<slug>/`.

    Returns the list of written paths (spec first, pipeline second). Creates parent
    directories as needed. Writes only text -- no Spark, no subprocess.
    """
    paths = artifact_paths(slug, base_dir)
    spec_text = render_spec(slug, name=name, storage_root=storage_root,
                            catalog=catalog, database=database)
    pipeline_text = render_pipeline_py(code)

    os.makedirs(os.path.dirname(paths["spec"]), exist_ok=True)
    os.makedirs(os.path.dirname(paths["pipeline"]), exist_ok=True)
    with open(paths["spec"], "w") as f:
        f.write(spec_text)
    with open(paths["pipeline"], "w") as f:
        f.write(pipeline_text)
    return [paths["spec"], paths["pipeline"]]
