"""Regression guard: the SDP arms actually LOAD their in-repo SKILL.md packs.

Root cause of zero completions: `_load_skill` searched `omnigent_skills_dir`, which
was unset (no env/config), so NO SKILL.md was found and the SDP agents got zero
skill guidance and hallucinated Databricks `dlt`. The fix defaults the brain's
skills dir to the in-repo `experiments/safe_agent_study/skills/` (resolved relative
to the study dir). This test proves, with NO env var set and NO network, that:

  * arm B's system prompt injects BOTH `=== LINKED SKILL: pyspark-sdp ===` and the
    spark-safety pack;
  * arm B1's system prompt injects the pyspark-sdp pack;
  * the loaded pyspark-sdp text carries the OSS API (`from pyspark import pipelines
    as dp`) and explicitly NOT the Databricks `import dlt`.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness.arm_manifest import load_arms          # noqa: E402
from harness.backends.live import AnthropicBrain, _DEFAULT_SKILLS_DIR  # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))


def _brain():
    # NO omnigent_skills_dir passed and (in this test process) no OMNIGENT_SKILLS
    # env -> must fall back to the in-repo default for the load to work.
    return AnthropicBrain(ARMS["A"].base_model_id, "TASK PROMPT")


def test_default_skills_dir_points_into_repo_and_exists():
    assert os.path.isdir(_DEFAULT_SKILLS_DIR), _DEFAULT_SKILLS_DIR
    assert os.path.isfile(os.path.join(_DEFAULT_SKILLS_DIR, "pyspark-sdp", "SKILL.md"))
    assert os.path.isfile(os.path.join(_DEFAULT_SKILLS_DIR, "spark-safety", "SKILL.md"))


def test_brain_defaults_to_in_repo_skills_when_env_unset():
    os.environ.pop("OMNIGENT_SKILLS", None)
    brain = _brain()
    assert os.path.normpath(brain.omnigent_skills_dir) == os.path.normpath(_DEFAULT_SKILLS_DIR)


def test_arm_B_system_prompt_injects_pyspark_sdp_only():
    # Locked design (paper §6.1, 2026-06-29): B = SDP + gate + pyspark-sdp, NO safety skill.
    # spark-safety was SCRAPPED everywhere (it moved silent-defect by 0.000). B must inject
    # pyspark-sdp and must NOT leak spark-safety -- same as the B1 ablation below.
    os.environ.pop("OMNIGENT_SKILLS", None)
    sysprompt = _brain()._system_prompt(ARMS["B"])
    assert "=== LINKED SKILL: pyspark-sdp ===" in sysprompt, "pyspark-sdp not injected for arm B"
    assert "spark-safety" not in sysprompt, "B must NOT load the safety skill (scrapped per §6.1)"


def test_arm_B1_system_prompt_injects_pyspark_sdp():
    os.environ.pop("OMNIGENT_SKILLS", None)
    sysprompt = _brain()._system_prompt(ARMS["B1"])
    assert "=== LINKED SKILL: pyspark-sdp ===" in sysprompt, "pyspark-sdp not injected for arm B1"
    # B1 has NO safety skill (ablation) -- it must not leak in.
    assert "spark-safety" not in sysprompt, "B1 must not load the safety skill (ablation)"


def test_loaded_skill_is_oss_sdp_not_databricks_dlt():
    text = _brain()._load_skill("pyspark-sdp")
    assert "from pyspark import pipelines as dp" in text, "OSS SDP import missing"
    assert "@dp.materialized_view" in text, "batch materialized_view guidance missing"
    assert "SparkSession.active()" in text, "session-acquisition guidance missing"
    # the whole point: explicitly steer AWAY from Databricks DLT
    assert "NOT Databricks" in text or "Databricks DLT" in text, \
        "skill does not warn against Databricks DLT"


def test_pyspark_sdp_is_api_only_no_task_solution_leak():
    """pyspark-sdp loads for BOTH B and B1, so it MUST teach only OSS SDP API
    mechanics -- never the task solution or the oracle-graded safety mitigations
    (that is the spark-safety treatment, arm B only). Leaking them into pyspark-sdp
    hands B1 the safety solution and destroys the B-vs-B1 ablation (inflates H1).
    Guard against the leak recurring by forbidding task/solution markers."""
    text = _brain()._load_skill("pyspark-sdp").lower()
    forbidden = ["gold_daily", "silver_orders", "quarantine", "dedup",
                 "merchant", "revenue", "medallion", "order_id", "from_json",
                 "withwatermark", "session.timezone"]
    leaked = [m for m in forbidden if m in text]
    assert not leaked, f"pyspark-sdp leaks task/solution content: {leaked}"
    # ...while STILL carrying the API mechanics it is supposed to teach.
    for marker in ("materialized_view", "sparksession.active", "transformations/**",
                   "dry-run"):
        assert marker in text, f"pyspark-sdp lost API marker {marker!r}"


def test_spark_safety_is_general_practice_not_task_solution():
    """spark-safety is the legitimate B-only treatment, but it must teach GENERAL
    safety practices -- not the task's exact schema/rollup. Forbid the exact-order
    schema keys and the oracle's defect/solution specifics; require the general
    practice markers."""
    text = _brain()._load_skill("spark-safety")
    low = text.lower()
    for marker in ("withWatermark", "dropDuplicates", "session.timeZone", "quarantine"):
        assert marker.lower() in low, f"spark-safety lost general-practice marker {marker!r}"
    forbidden = ["order_id", "merchant_id", "gold_daily", "silver_orders",
                 "raw_schema", "revenue"]
    leaked = [m for m in forbidden if m in low]
    assert not leaked, f"spark-safety leaks task-specific solution content: {leaked}"


def test_imperative_arms_load_no_skill():
    os.environ.pop("OMNIGENT_SKILLS", None)
    # B2 dropped: withdrawn to arms/supplementary per paper §6.1 (2026-06-29).
    for aid in ("A",):
        sysprompt = _brain()._system_prompt(ARMS[aid])
        assert "=== LINKED SKILL:" not in sysprompt, f"arm {aid} should link no skill"


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback; failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
