"""Agent PR author -- the ONLY thing the agent runs in the GitOps loop.

The agent's entire surface is: author a declarative SDP artifact, then `git` + `gh`.
It NEVER receives a Spark session. That is the whole thesis of this slice: the
agent's surface is *files + git + PR*, and only CI / the controller ever holds a
Spark session. Imperative pipelines cannot be GitOps'd this way because the agent
*owns* the session -- there is nothing declarative to hand to a reconciler.

The brain is the study's REAL brain: `AnthropicBrain` from
`harness/backends/live.py`, configured from the study's Arm-B manifest (SDP paradigm
+ dry-run gate + safety skill). We call `brain.propose(...)` to get the agent's SDP
code, render it into the versioned artifact via `sdp_artifact.py`, and then do ONLY:

    git checkout -b agent/gitops/<slug>-<ts>
    git add pipeline-definitions/<slug>/...
    git commit            (with a Co-authored-by: omnigent trailer)
    git push
    gh pr create          (base: --base-branch)

HARD SAFETY BOUNDARY (real, not cosmetic -- enforced in code, asserted in tests):
  * this module imports NO pyspark (verify: `pyspark` never enters sys.modules);
  * it REFUSES TO RUN (exit nonzero) if SPARK_REMOTE is set in the environment;
  * it never instantiates ConnectExecutor / opens a SparkSession;
  * it never invokes spark-pipelines / spark-submit / cli.py;
  * every subprocess call is funneled through an ALLOWLIST of `git` and `gh` only.

CLI:  agent_pr_author.py --task <task.json> --pipeline-slug <slug> --base-branch main
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import List, Optional, Sequence

# gitops_demo/ is this file's dir; the study root is its parent (for harness imports).
_HERE = os.path.dirname(os.path.abspath(__file__))
_STUDY_ROOT = os.path.dirname(_HERE)
if _STUDY_ROOT not in sys.path:
    sys.path.insert(0, _STUDY_ROOT)

import sdp_artifact  # noqa: E402  (local module; no Spark)

# The arm whose loop config the agent runs under: SDP + dry-run gate + safety skill.
ARM_B_MANIFEST = os.path.join(_STUDY_ROOT, "arms", "B.json")

# The ONLY binaries the agent surface may shell out to. Anything else is refused.
ALLOWED_BINARIES = ("git", "gh")

# Commit trailer required by the task: attribute the omnigent co-author.
COAUTHOR_TRAILER = "Co-authored-by: omnigent <noreply@omnigent.ai>"


class SafetyBoundaryError(RuntimeError):
    """Raised when the agent surface would cross into Spark/session territory."""


# --------------------------------------------------------------------------- #
# Safety boundary
# --------------------------------------------------------------------------- #
def assert_no_spark_remote(env: Optional[dict] = None) -> None:
    """Refuse to run if SPARK_REMOTE is set. The agent must never have a session.

    SPARK_REMOTE in the env is the signal that *something* wired a Spark Connect
    endpoint into this process. The agent author is, by design, blind to Spark --
    if that variable is present we are in the wrong identity (the controller's), so
    we abort loudly rather than risk the agent reaching a cluster.
    """
    e = os.environ if env is None else env
    if e.get("SPARK_REMOTE"):
        raise SafetyBoundaryError(
            "SPARK_REMOTE is set; the agent PR author must NEVER hold a Spark "
            "session. Refusing to run. (Only CI / the controller may set "
            "SPARK_REMOTE.)"
        )


def _binary_name(argv0: str) -> str:
    """Basename without extension, lowercased -- the allowlist comparison key."""
    return os.path.splitext(os.path.basename(str(argv0)))[0].lower()


def run_cmd(argv: Sequence[str], *, cwd: Optional[str] = None,
            check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    """Run an ALLOWLISTED command (git/gh only). Any other binary is refused.

    This is the single choke point for every subprocess the agent surface makes.
    It also re-checks the SPARK_REMOTE boundary on every call so a session endpoint
    can never be injected mid-run, and it strips SPARK_REMOTE from the child env as
    defense in depth.
    """
    if not argv:
        raise SafetyBoundaryError("refusing to run an empty command")
    name = _binary_name(argv[0])
    if name not in ALLOWED_BINARIES:
        raise SafetyBoundaryError(
            f"command {argv[0]!r} is not in the allowlist {ALLOWED_BINARIES}; "
            "the agent surface may only run git and gh."
        )
    assert_no_spark_remote()
    child_env = dict(os.environ)
    child_env.pop("SPARK_REMOTE", None)  # belt-and-suspenders: never pass a session
    return subprocess.run(
        list(argv), cwd=cwd, env=child_env, check=check,
        capture_output=capture, text=True,
    )


# --------------------------------------------------------------------------- #
# Brain (the study's real Arm-B brain)
# --------------------------------------------------------------------------- #
def load_arm_b():
    """Load the study's Arm-B manifest (SDP + dry-run gate + safety skill)."""
    from harness.arm_manifest import load_arm
    return load_arm(ARM_B_MANIFEST)


def build_brain(task_prompt: str):
    """Construct the study's AnthropicBrain with Arm-B sampling.

    Importing `AnthropicBrain` does NOT import pyspark (live.py imports anthropic and
    pyspark lazily, inside methods). Constructing the brain makes NO network call --
    the Anthropic client is created lazily on the first `propose`.
    """
    from harness.backends.live import AnthropicBrain
    from harness.arm_manifest import sampling_kwargs
    arm = load_arm_b()
    brain = AnthropicBrain(arm.base_model_id, task_prompt,
                           sampling=sampling_kwargs(arm))
    return brain, arm


# --------------------------------------------------------------------------- #
# Propose + render (the no-Spark file-generation path)
# --------------------------------------------------------------------------- #
def _load_task(task_path: str) -> dict:
    with open(task_path) as f:
        return json.load(f)


def _task_prompt(task: dict) -> str:
    """The prompt text handed to the brain. Prefers an explicit `prompt`, else
    composes a minimal brief from title/description so the slice is self-contained."""
    if task.get("prompt"):
        return str(task["prompt"]).strip()
    title = task.get("title", task.get("id", "pipeline"))
    desc = task.get("description", "")
    return f"# Task — {title}\n\n{desc}".strip()


def propose_and_render(brain, arm, task: dict, slug: str, artifact_base: str,
                       *, storage_root: Optional[str] = None) -> List[str]:
    """Ask the brain for SDP code and render the versioned artifact. No Spark.

    `artifact_base` is the directory that CONTAINS `pipeline-definitions/` (the
    gitops_demo directory for a real run; a tmp dir under test). Files land at
    `<artifact_base>/pipeline-definitions/<slug>/...`, which under a real run is the
    canonical `experiments/safe_agent_study/gitops_demo/pipeline-definitions/<slug>/`
    that CI's `changed_pipelines.py` and the workflow `paths:` filter look for.

    Returns the list of written file paths (absolute). This is the pure
    file-generation path the tests exercise with a mocked `brain.propose`.
    """
    from harness.backends.base import LoopState
    # Re-check the boundary IMMEDIATELY before the brain runs: a SPARK_REMOTE injected
    # mid-process (after startup) must not slip past into the proposal step.
    assert_no_spark_remote()
    state = LoopState(
        task=task.get("id", slug),
        seed=0,
        workspace=os.path.join(artifact_base, sdp_artifact.PIPELINE_DEFINITIONS_DIR, slug),
        dataset_path=task.get("dataset_path", ""),
        output_table=task.get("output_table", "agent_output"),
    )
    proposal = brain.propose(state, arm)
    if not getattr(proposal, "code", "").strip():
        raise RuntimeError("brain returned an empty proposal; nothing to author")
    written = sdp_artifact.write_artifact(
        artifact_base, slug, proposal.code,
        name=task.get("pipeline_name") or f"gitops_demo__{slug}",
        storage_root=storage_root,
    )
    return written


# --------------------------------------------------------------------------- #
# Git + gh (the only side effects)
# --------------------------------------------------------------------------- #
def _timestamp() -> str:
    """UTC compact timestamp for the branch name. Uses git (allowlisted) as the
    clock so we add no extra dependency and stay inside the subprocess allowlist."""
    out = run_cmd(["git", "show", "-s", "--format=%cd",
                   "--date=format-local:%Y%m%d-%H%M%S", "HEAD"]).stdout.strip()
    return out or "00000000-000000"


def open_pr(repo_root: str, slug: str, written: List[str], base_branch: str,
            task: dict) -> str:
    """Create the branch, commit the artifact, push, and open the PR. git/gh only.

    Returns the branch name. Every command goes through `run_cmd` (allowlist +
    SPARK_REMOTE guard).
    """
    ts = _timestamp()
    branch = f"agent/gitops/{slug}-{ts}"
    rels = [os.path.relpath(p, repo_root) for p in written]

    run_cmd(["git", "checkout", "-b", branch], cwd=repo_root)
    run_cmd(["git", "add", *rels], cwd=repo_root)

    title = f"gitops(sdp): add pipeline `{slug}`"
    body = _pr_body(slug, task, rels)
    commit_msg = f"{title}\n\n{body}\n\n{COAUTHOR_TRAILER}\n"
    run_cmd(["git", "commit", "-m", commit_msg], cwd=repo_root)
    run_cmd(["git", "push", "-u", "origin", branch], cwd=repo_root)
    run_cmd(["gh", "pr", "create", "--base", base_branch, "--head", branch,
             "--title", title, "--body", body], cwd=repo_root)
    return branch


def _pr_body(slug: str, task: dict, rels: List[str]) -> str:
    files = "\n".join(f"- `{r}`" for r in rels)
    return (
        f"Agent-authored declarative SDP pipeline **{slug}**.\n\n"
        f"Task: `{task.get('id', slug)}` — {task.get('title', '')}\n\n"
        "## What this PR is\n"
        "A declarative pipeline artifact (spec + transform). The CI dry-run gate "
        "(`spark-pipelines dry-run`) runs the REAL structural analysis on it; on "
        "merge, the controller reconciles it with `spark-pipelines run`.\n\n"
        "## Files\n"
        f"{files}\n\n"
        "## Safety boundary\n"
        "This PR was authored by an agent that **never held a Spark session** — its "
        "entire surface was files + git + PR. Only CI / the controller hold "
        "`SPARK_REMOTE`. See `gitops_demo/README.md`.\n"
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Agent SDP PR author (no Spark session).")
    p.add_argument("--task", required=True, help="Path to the task JSON.")
    p.add_argument("--pipeline-slug", required=True, help="Slug for the pipeline dir.")
    p.add_argument("--base-branch", default="main", help="PR base branch.")
    p.add_argument("--repo-root", default=None,
                   help="Repo root to write into / run git in (default: git toplevel).")
    p.add_argument("--storage-root", default=None,
                   help="Override SDP storage root (default: local file:// demo root).")
    p.add_argument("--no-pr", action="store_true",
                   help="Render the artifact only; skip git/gh (for local inspection).")
    return p.parse_args(argv)


def _git_toplevel() -> str:
    return run_cmd(["git", "rev-parse", "--show-toplevel"]).stdout.strip()


def main(argv: Optional[Sequence[str]] = None) -> int:
    # FIRST THING: refuse to run with a Spark endpoint in the environment.
    try:
        assert_no_spark_remote()
    except SafetyBoundaryError as e:
        print(f"SAFETY BOUNDARY: {e}", file=sys.stderr)
        return 2

    args = parse_args(argv)
    task = _load_task(args.task)
    slug = sdp_artifact.slugify(args.pipeline_slug)
    repo_root = args.repo_root or _git_toplevel()
    # Artifacts are written under THIS gitops_demo directory so they land at the
    # canonical experiments/safe_agent_study/gitops_demo/pipeline-definitions/<slug>/
    # path that CI discovery + the workflow `paths:` filter expect. git add/commit
    # below address them via repo_root-relative paths.
    artifact_base = _HERE

    brain, arm = build_brain(_task_prompt(task))
    written = propose_and_render(brain, arm, task, slug, artifact_base,
                                 storage_root=args.storage_root)
    print("rendered artifact:")
    for p in written:
        print(f"  {os.path.relpath(p, repo_root)}")

    if args.no_pr:
        print("--no-pr: skipping git/gh.")
        return 0

    branch = open_pr(repo_root, slug, written, args.base_branch, task)
    print(f"opened PR from branch {branch} (base {args.base_branch}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
