"""Unit tests for the SDP artifact renderer (`sdp_artifact.py`).

Asserts the rendered `spark-pipeline.yml` has the exact shape the SDP CLI accepts:
name / storage / catalog / database / a `libraries` glob over `transformations/**`;
and that `write_artifact` lays the two files out under
`pipeline-definitions/<slug>/`. No Spark, no network.
"""
import os
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
GITOPS_DEMO = os.path.dirname(HERE)
sys.path.insert(0, GITOPS_DEMO)

import sdp_artifact  # noqa: E402


AGENT_CODE = (
    "from pyspark import pipelines as dp\n"
    "from pyspark.sql import functions as F\n\n"
    "@dp.table\n"
    "def orders_silver():\n"
    "    return spark.read.json(spark.conf.get('input'))\n"
)


def test_slugify_is_branch_and_path_safe():
    assert sdp_artifact.slugify("Orders Silver/Gold #1") == "orders-silver-gold-1"
    assert sdp_artifact.slugify("") == "pipeline"
    assert sdp_artifact.slugify("  already-ok  ") == "already-ok"


def test_spec_declares_required_shape():
    spec_text = sdp_artifact.render_spec("orders_silver_gold")
    spec = yaml.safe_load(spec_text)

    # Exactly the fields the CLI's unpack_pipeline_spec allows.
    assert set(spec) == {"name", "storage", "catalog", "database", "libraries"}
    assert spec["name"] == "gitops_demo__orders_silver_gold"
    assert spec["catalog"] == "spark_catalog"
    assert spec["database"] == "gitops_demo"
    # local-demo storage points under the documented local root, namespaced by slug.
    assert spec["storage"] == (
        "file:///tmp/safe-spark-agents-gitops/orders_silver_gold/storage"
    )


def test_libraries_is_a_transformations_glob():
    spec = yaml.safe_load(sdp_artifact.render_spec("p1"))
    libs = spec["libraries"]
    assert isinstance(libs, list) and len(libs) == 1
    assert libs[0] == {"glob": {"include": "transformations/**"}}


def test_render_spec_overrides():
    spec = yaml.safe_load(
        sdp_artifact.render_spec("p2", name="custom_name",
                                 storage_root="s3a://bucket/x",
                                 catalog="other_cat", database="other_db")
    )
    assert spec["name"] == "custom_name"
    assert spec["storage"] == "s3a://bucket/x/p2/storage"
    assert spec["catalog"] == "other_cat"
    assert spec["database"] == "other_db"


def test_pipeline_py_contains_agent_code_verbatim():
    rendered = sdp_artifact.render_pipeline_py(AGENT_CODE)
    # the agent's code survives verbatim (modulo the provenance header + trailing nl)
    assert AGENT_CODE.rstrip() in rendered
    assert rendered.startswith("# Declarative SDP transform")


def test_write_artifact_layout(tmp_path):
    written = sdp_artifact.write_artifact(str(tmp_path), "demo_slug", AGENT_CODE)
    spec_path = tmp_path / "pipeline-definitions" / "demo_slug" / "spark-pipeline.yml"
    pipe_path = (tmp_path / "pipeline-definitions" / "demo_slug"
                 / "transformations" / "pipeline.py")

    assert [os.path.abspath(p) for p in written] == [
        os.path.abspath(str(spec_path)), os.path.abspath(str(pipe_path))
    ]
    assert spec_path.exists() and pipe_path.exists()

    spec = yaml.safe_load(spec_path.read_text())
    assert spec["name"] == "gitops_demo__demo_slug"
    assert AGENT_CODE.rstrip() in pipe_path.read_text()
