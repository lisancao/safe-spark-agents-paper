"""Arm manifests + the identical-except-loop invariant (pre-reg §3).

Pre-reg §3 is the validity crux: "the loop is the ONLY manipulated variable."
All arms must share the same base model, the same task prompt text, the same
input data per matched seed, and the same Connect backend; only the development
loop differs.

We make that auditable instead of hoping for it. Each arm is one JSON file in
`arms/` carrying the full manifest (model, prompt ref, skills, allowed commands,
dry-run-gate, max iterations, paradigm). `load_arms()` then runs
`assert_identical_except_loop()`, which FAILS LOUDLY if any two arms differ on a
shared-by-design field. The only fields permitted to vary across arms are the
declared LOOP fields:

    paradigm        sdp | imperative_pyspark
    dry_run_gate    bool   (structural pre-execution gate on/off)
    safety_skill    bool   (the safety skill linked or not)
    skills          list   (skill packs linked into the loop)
    allowed_commands list  (commands the agent may run in the loop)

Everything else -- base_model_id, task_prompt_ref, max_iterations, sampling
params -- MUST be byte-identical across all arms, so a difference in outcome is
attributable to the loop, not to a model/prompt/budget confound.
"""
from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List

# Fields that DEFINE the loop and are therefore allowed to differ across arms.
LOOP_FIELDS = ("paradigm", "dry_run_gate", "safety_skill", "skills", "allowed_commands")

# Fields that MUST be identical across all arms (the controlled variables).
SHARED_FIELDS = ("base_model_id", "task_prompt_ref", "max_iterations",
                 "temperature", "top_p")

# Sampling control (single source of truth, shared with backends/live.py).
# The pre-registration fixes NEITHER temperature nor top_p by name (its determinism
# control is fixed seeds + the random-effects model, §9); the manifests carry
# temperature=0.0 (the conventional determinism knob) and top_p=1.0 (the API
# default), BOTH recorded as controlled-variable provenance and validated identical
# across arms.
# What is actually TRANSMITTED to the API is decided per model family in
# backends/live.py `build_request` (see DEVIATIONS D-6):
#   * the study base model claude-opus-4-8 REJECTS temperature/top_p/top_k/
#     budget_tokens (hard 400) and uses ADAPTIVE thinking -> NO sampling param is
#     transmitted at all (thinking={"type":"adaptive"});
#   * the legacy Claude 4.x family (e.g. claude-sonnet-4-6) rejects sending BOTH
#     temperature and top_p, so it transmits only temperature.
# SAMPLING_SENT below is therefore the controlled sampling value CARRIED INTO the
# brain as provenance (`sampling_kwargs`); whether it reaches the wire is the
# family decision above. Keeping it temperature-only preserves the legacy invariant
# and means the carried value is always a validated-identical shared field.
SAMPLING_CONTROLLED = ("temperature", "top_p")   # both recorded + validated identical
SAMPLING_SENT = ("temperature",)                 # controlled value carried to the brain
# Invariant: the carried sampling value is necessarily a validated-identical shared
# field, so it can never silently diverge across arms.
assert set(SAMPLING_SENT) <= set(SAMPLING_CONTROLLED) <= set(SHARED_FIELDS)

VALID_PARADIGMS = ("sdp", "imperative_pyspark")


def sampling_kwargs(m: "ArmManifest") -> Dict[str, Any]:
    """The controlled sampling value carried into the brain for this arm -- the
    SAMPLING_SENT subset (temperature only). Whether it is actually transmitted to
    the API is a per-model-family decision in `build_request`: the opus-4-x base
    transmits NONE (adaptive thinking), the legacy sonnet family transmits only
    temperature (never both -> no 'both specified' 400). Identical across arms by
    the shared-field invariant above (DEVIATIONS D-6)."""
    return {k: getattr(m, k) for k in SAMPLING_SENT}


@dataclass
class ArmManifest:
    arm_id: str
    # --- shared (controlled) -------------------------------------------
    base_model_id: str
    task_prompt_ref: str          # reference to the SHARED task prompt template
    max_iterations: int
    temperature: float = 0.0
    top_p: float = 1.0
    # --- loop (manipulated) --------------------------------------------
    paradigm: str = "imperative_pyspark"
    dry_run_gate: bool = False
    safety_skill: bool = False
    skills: List[str] = field(default_factory=list)
    allowed_commands: List[str] = field(default_factory=list)
    # --- documentation -------------------------------------------------
    description: str = ""

    def loop_signature(self) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in LOOP_FIELDS}

    def shared_signature(self) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in SHARED_FIELDS}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ArmManifest":
        known = {
            "arm_id", "base_model_id", "task_prompt_ref", "max_iterations",
            "temperature", "top_p", "paradigm", "dry_run_gate", "safety_skill",
            "skills", "allowed_commands", "description",
        }
        unknown = set(d) - known
        if unknown:
            raise ValueError(f"arm {d.get('arm_id')!r}: unknown manifest fields {sorted(unknown)}")
        if d.get("paradigm") not in VALID_PARADIGMS:
            raise ValueError(f"arm {d.get('arm_id')!r}: paradigm must be one of {VALID_PARADIGMS}")
        return ArmManifest(**{k: v for k, v in d.items() if k in known})


def load_arm(path: str) -> ArmManifest:
    with open(path) as f:
        return ArmManifest.from_dict(json.load(f))


def load_arms(arms_dir: str) -> Dict[str, ArmManifest]:
    """Load all arm manifests and enforce the identical-except-loop invariant."""
    arms: Dict[str, ArmManifest] = {}
    for p in sorted(glob.glob(os.path.join(arms_dir, "*.json"))):
        m = load_arm(p)
        arms[m.arm_id] = m
    if not arms:
        raise FileNotFoundError(f"no arm manifests found under {arms_dir}")
    assert_identical_except_loop(arms)
    return arms


def assert_identical_except_loop(arms: Dict[str, ArmManifest]) -> None:
    """Raise if any controlled (shared) field differs across arms (pre-reg §3).

    This is the programmatic guarantee that the experiment manipulates ONLY the
    loop. If it ever raises, the run must not proceed: the comparison would be
    confounded.

    Sampling is still fully checked even though only ONE sampling knob is sent to
    the API (Claude 4.x rejects both): BOTH SAMPLING_CONTROLLED params remain in
    SHARED_FIELDS and are validated identical across arms, and the SAMPLING_SENT
    subset is asserted (at module load) to be a subset of SHARED_FIELDS -- so the
    knob actually transmitted is provably one of the fields validated here. This
    closes any path to silently stopping the sampling check.
    """
    # defensive: the param actually sent to the API must be a validated field.
    assert set(SAMPLING_SENT) <= set(SHARED_FIELDS), \
        "a sampling param is sent to the API but not validated identical-across-arms"
    if len(arms) < 2:
        return
    ref_id, ref = next(iter(arms.items()))
    ref_shared = ref.shared_signature()
    problems: List[str] = []
    for arm_id, m in arms.items():
        for k in SHARED_FIELDS:
            if getattr(m, k) != ref_shared[k]:
                problems.append(
                    f"arm {arm_id!r}.{k}={getattr(m, k)!r} != arm {ref_id!r}.{k}={ref_shared[k]!r}"
                )
    if problems:
        raise ValueError(
            "ARMS ARE NOT IDENTICAL-EXCEPT-LOOP (pre-reg §3 violated):\n  "
            + "\n  ".join(problems)
            + "\nThe loop must be the only manipulated variable; refusing to run."
        )

    # Sanity: the loop signatures should actually DIFFER (else two arms are dupes).
    seen: Dict[str, str] = {}
    for arm_id, m in arms.items():
        sig = json.dumps(m.loop_signature(), sort_keys=True)
        if sig in seen:
            raise ValueError(
                f"arms {seen[sig]!r} and {arm_id!r} have identical loop signatures; "
                "they are not distinct arms."
            )
        seen[sig] = arm_id


def arm_contrasts() -> List[tuple]:
    """The 5 pre-registered arm contrasts for Holm correction (pre-reg §7)."""
    return [("A", "B"), ("A", "B1"), ("A", "B2"), ("B", "B1"), ("B", "B2")]
