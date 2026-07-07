# skills/ — runtime mount point (the SDP skill is LINKED, not embedded)

This directory is intentionally empty in the image. The SDP skill pack (`pyspark-sdp`) is
**linked at runtime**, never copied into the image, because that repo is a work in progress and
a baked copy would go stale. `entrypoint.sh` resolves the skill in one of two ways:

- **Dev (bind-mount):** mount your local checkout here and edits are live:
  `-v ~/repos/pyspark-sdp/.claude/skills:/opt/spark-omnigent/skills:ro`
- **Built image (clone):** if nothing is mounted, the entrypoint shallow-clones
  `SDP_SKILL_REPO@SDP_SKILL_REF` (default `lisancao/pyspark-sdp@main`). Pin `SDP_SKILL_REF` to a
  tag/sha for a reproducible fleet, and pass `SDP_SKILL_TOKEN` if the repo is private.

`OMNIGENT_SKILLS` is set to whichever path won, and points the agent harness at the skill.
