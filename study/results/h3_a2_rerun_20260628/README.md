# H3 A2 re-run (2026-06-28) — clean B-vs-A2 paradigm contrast

Re-ran the FULL A2 (imperative+gate+skill) arm on tag `instrument-v3.1` after the
dead-session read-back fix (#33). On the old instrument only 2/66 A2 cells completed
(47 false `max_iterations` from the read-back bug + 15 race); here 61/66 complete.

- `results.a2_full.jsonl` — new A2 data, 66 cells (22 tasks x seeds 42/1337/2718), `--backend local`.
- `results.h3_combined.jsonl` — new A2 + existing clean B/B1 (198 cells) used for HEADLINE.
- `HEADLINE.md/json`, `QUARANTINE.md` — `analysis/analyze.py --assume-backend local`.

Headline (N=3): silent-defect rate A2=B=B1=0.348 (23/66) — paradigm and safety-skill are
NULL on silent defects; conciseness B-vs-A2 ~42% fewer LOC / ~38% fewer AST nodes (tight CIs).
B and B1 are the existing clean data; only A2 was re-run (SDP arms use a different read-back path).
