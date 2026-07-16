# Vendored: Omnigent Kubernetes deploy manifests

Vendored verbatim from the official Omnigent repository so this repo is a self-contained client
handoff: a client does not need to clone a second repo to deploy the Omnigent delivery layer
(Section 4 / CLIENT_RUNBOOK.md Section 6).

Source  : https://github.com/omnigent-ai/omnigent  (path: deploy/kubernetes/)
Commit  : 046246fb9866cf32745858e076df04875b87457e
Vendored: 2026-07-14

This is a PINNED SNAPSHOT. Omnigent moves fast (the local ~/omnigent checkout was ~800 commits
behind), so re-vendor periodically for the latest overlays and fixes:

  git clone --depth 1 https://github.com/omnigent-ai/omnigent /tmp/omnigent
  rsync -a --delete /tmp/omnigent/deploy/kubernetes/ deploy/kubernetes/   # keep this VENDORED.md
  # then update the Commit + Vendored lines above.

The upstream deploy/kubernetes/README.md (kept in this dir) is the full deploy guide; see
CLIENT_RUNBOOK.md Section 6 for how it composes with the Section 3 platform + custodian.
