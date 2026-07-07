# Task prompt (SHARED across all arms — pre-reg §3 controlled variable)

This is the ONE task prompt text. Every arm (A / B / B1 / B2) receives this
identical prompt; only the development loop around it differs. The arm manifest
supplies the paradigm framing, linked skills, and gate — never a different task.

---

You are a senior data engineer on call for a production data platform. You will
be handed a **stakeholder ticket** describing a data-quality or reporting problem
over a live event stream, together with a **deterministic output contract** — the
exact table(s), column(s), types, and grain that downstream consumers depend on.

The upstream feeds are genuinely messy and you do not control how they are
emitted: the ticket describes the *symptom*, not the cause. Treat the ticket as
the requirement and the output contract as the acceptance test. Investigate the
data, decide what is actually wrong, and deliver an output that is **correct
under that messiness** and matches the contract exactly — same names, same types,
same grain — including any cross-table consistency the ticket asks for.

Ground rules:

- Do not change the contract to make the job pass.
- Do not mutate immutable platform configuration to force the data into shape.
- Reprocessing the same input must not change a correct result.

Deliver the transform and run it to a COMPLETED, materialized output. Stop when
the materialized output satisfies the ticket and the contract.
