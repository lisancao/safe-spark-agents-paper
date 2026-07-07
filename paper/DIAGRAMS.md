# Paper diagrams — design brief (hand to Claude design)

Purpose: make the paper legible at a glance — it's both a *controlled safety study* (Section 1)
and a *systems demonstration* (Sections 2–3). Below: a prioritized set, each with purpose,
placement, exact content/data (accurate as of 2026-07-06), layout, a draft caption, and design
notes. Mermaid drafts are included where a flow is clearer as code — treat them as starting points.

**Shared visual language (keep consistent across all figures):**
- **Imperative (Arm A)** = warm (amber/orange). **SDP (Arm B)** = cool (blue/teal).
- **Gate / caught-early** = green. **Shipped defect / gap** = red. **Proposed / not-yet** = grey dashed.
- **Zones:** Untrusted agent = grey; Governed control = blue; Data/execution = teal.
- One accent per figure; avoid rainbow. Sans-serif; mono for code/identifiers.

---

## TIER 1 — the core story (do these first)

### D1. The control boundary + agent-native dev loop  *(Section 2, the thesis)*
**Type:** horizontal flow with a hard vertical "boundary" line separating *authoring* from *execution*.
**Shows:** the one idea the whole paper rests on — the agent proposes *inert desired-state*; a
governed system validates and executes; the agent never holds a session or touches data.
**Content (left→right):**
1. **Agent (Zone U, untrusted)** — writes an *inert artifact* (an SDP spec / `transformations/pipeline.py`). Label: "authors desired-state · holds no Spark session · holds no creds."
2. **‖ BOUNDARY ‖** (bold vertical divider — the load-bearing element).
3. **Structural dry-run gate** (green) — "validates the graph *before any data is processed*; rejects structural defects (D1/D4/D5)."
4. **Reconciler / controller** (Zone C, governed) — "the system, not the agent, acquires the session and materializes."
5. **Execution** (Zone D) — Spark Connect → executor pods → data.
**Caption:** *The control boundary: the agent authors inert desired-state; the governed system validates (dry-run gate) and executes. Authoring ⊥ execution — the agent never holds a live session. Declarative pipelines make this separation possible; Connect enforces it.*

```mermaid
flowchart LR
  subgraph U["Zone U — untrusted agent"]
    A["AI agent<br/>authors inert spec<br/>(no session, no creds)"]
  end
  subgraph C["Zone C — governed control"]
    G["Structural<br/>dry-run gate<br/>(pre-execution)"]
    R["Reconciler<br/>acquires session,<br/>materializes"]
  end
  subgraph D["Zone D — execution / data"]
    X["Spark Connect →<br/>executor pods → data"]
  end
  A -->|"desired-state<br/>(inert artifact)"| G
  G -->|"structural OK"| R
  G -.->|"reject D1/D4/D5<br/>before any data"| A
  R --> X
  classDef u fill:#eee,stroke:#999; classDef c fill:#dbeafe,stroke:#2563eb; classDef d fill:#ccfbf1,stroke:#0d9488;
  class A u; class G,R c; class X d;
```

### D2. Experiment design — one manipulation, two arms  *(Section 1 §2/§6)*
**Type:** two-column comparison + a corpus/substrate strip beneath.
**Shows:** the clean design — IV = *paradigm only*; everything else held constant; the full cell count.
**Content:**
- **Arm A (amber): "bare imperative"** — paradigm: imperative · gate: none · skill: none · role: imperative as it natively is.
- **Arm B (blue): "SDP"** — paradigm: declarative · gate: framework dry-run (intrinsic) · skill: `pyspark-sdp` · role: declarative treatment.
- **Held constant** (center band): model (`claude-opus-4-8`), corpus, seeds, prompt, temperature, iteration cap.
- **Corpus/substrate strip:** 22 tasks (7 Low / 8 Med / 7 High) × 12 seeds × 2 arms = **528 cells**. Substrate: **local** for H1/H2/H4/H5; **EKS uniform Connect** for H3.
- Small footnote chip: "+1 non-CDC High addendum (v3.1)."
**Caption:** *One manipulation (paradigm), everything else fixed. 528 cells (22 tasks × 12 seeds × 2 arms). The gate is part of the treatment, not a covariate — that asymmetry is the finding (F1).*
**Design note:** emphasize the single-variable manipulation; the "gate: none vs intrinsic" row is the crux.

### D3. Defect taxonomy — what a gate can and cannot catch  *(Section 1 §3.2, load-bearing)*
**Type:** 3-band grouping (structural / semantic-silent / state) crossed with "where caught."
**Shows:** *why* structural gates catch crashes but not wrongness — the conceptual spine of H1.
**Content — three bands:**
- **Structural (green, gate-catchable):** D1 unresolved column · D4 broken DAG / missing upstream · D5 immutable-config mutation. → caught at **dry-run**, before data.
- **Semantic / silent (red, un-gateable):** D2 timestamp misparse · D6 nondeterministic dedup · D7 timezone/day-bucket · D8 silent row-drop. → only visible in **completed output** → these *are* the silent defects.
- **State (grey):** D3 unwatermarked dedup · D9 unbounded state. → not offline-scored.
- Right edge: detection-stage axis `dry_run → runtime → never (shipped)`.
**Caption:** *Defects split by what a structural gate can see. The gate catches structural failures (D1/D4/D5) pre-execution; semantic defects (D2/D6/D7/D8) are un-gateable by construction and ship as silent defects. Any paradigm effect can appear only in the structural band — or in how well the agent is taught to handle the semantic one (see D5-fig).*

---

## TIER 2 — the findings  *(Section 1 §4)*

### D4. H1 headline — structural-catch & failure-mode shift
**Type:** side-by-side "where caught" stacked bars (A vs B), structural defects only.
**Data (exact):**
- **Arm A (no gate):** gate 0 · runtime 4 · shipped 0.
- **Arm B (framework gate):** **gate 79** · runtime 30 · shipped 0. Iteration-level: **B intercepts 353** error events at the gate; **A intercepts 0**.
**Shows:** SDP catches structural failures *early* (at the gate, pre-execution); imperative surfaces them at runtime. The 79-vs-0 is the safety mechanism.
**Caption:** *H1.1: the SDP gate intercepts 79 structural defects (353 iteration-level) before any data is processed; bare imperative has no gate (0). Powered, N=264 (task,seed) cells.*

### D5. The D7 finding — a safety property that *causes* a defect, and the fix  *(§4.1.2 — the standout)*
**Type:** two-lane flow (imperative vs SDP) → outcome, then a "skill fix" arrow. This is the most
interesting result; give it space.
**Content:**
- **Imperative lane (amber):** `spark.conf.set(session.timeZone, "UTC")` → `to_date(...)` → **correct UTC day** → D7 = 0.
- **SDP lane (blue):** wants the same → **immutable-config gate blocks `session.timeZone`** (D5 property, `CANNOT_MODIFY_CONFIG`) → agent hand-rolls tz-dependent, payment/rate-asymmetric math → **wrong day ships** → **D7 = 7**.
- **The fix (green arrow, below):** teach `pyspark-sdp` the column-level UTC idiom → re-run → **D7: 7 → 0**. Label: "skill-induced, not paradigm-inherent (`results.tzfix.jsonl`)."
**Caption:** *SDP's immutable-config safety property removes the one-line timezone fix imperative uses, so the agent hand-rolls broken day-math and ships D7. Teaching a column-level UTC idiom closes it entirely (7→0) — the raw "SDP less safe" gap is a skill gap, not the paradigm.*

```mermaid
flowchart TD
  subgraph IMP["Imperative (A)"]
    I1["set session.timeZone=UTC<br/>(one line)"] --> I2["to_date(...) → correct UTC day"] --> I3["D7 = 0 ✓"]
  end
  subgraph SDP["SDP (B) — base skill"]
    S1["wants session.timeZone=UTC"] --> S2["BLOCKED: immutable-config<br/>(D5, CANNOT_MODIFY_CONFIG)"] --> S3["hand-rolls tz-dependent,<br/>asymmetric day-math"] --> S4["D7 = 7 ✗ (ships)"]
  end
  FIX["Fix: teach pyspark-sdp the<br/>UTC column idiom → D7: 7 → 0"]
  S4 -.->|skill-attribution test| FIX
  classDef a fill:#fef3c7,stroke:#d97706; classDef b fill:#dbeafe,stroke:#2563eb; classDef f fill:#dcfce7,stroke:#16a34a;
  class I1,I2,I3 a; class S1,S2,S3,S4 b; class FIX f;
```

### D6. Silent-defect composition — where the gap actually is  *(§4.1.1)*
**Type:** grouped bar, per semantic class, A vs B.
**Data (ships, A / B):** D2 1/3 · **D6 38/39 (a wash)** · **D7 0/7 (SDP-specific)** · D8 51/57.
**Shows:** the raw B-worse silent-defect gap is carried by D7 + D8, *not* D6 (the largest class, tied). Pairs with D5 (D7 is skill-attributable).
**Caption:** *The A−B silent-defect gap is D7 (timezone) + D8 (row-drop); D6 (dedup, the largest class) is a wash. D7 is skill-attributable (fig. D5).*

### D7. Conciseness (H4) — SDP writes ~half the code
**Type:** two paired bars (LOC, AST), A vs B, with % labels.
**Data:** LOC **B 67.9 / A 134.0** (~49% fewer); AST nodes **B 615 / A 1106** (~44% smaller). Both CIs clear of zero.
**Caption:** *The declarative agent writes ~half the code (−49% LOC, −44% AST), paired over (task,seed).*
**Optional companion chip:** tokens **A 11.5k / B 26.5k** (SDP ~2.3× — iterates more) — the honest counter-cost.

---

## TIER 3 — what we built (the system)  *(Sections 2–3)*

### D8. EKS Spark-Connect topology — the governed ingress  *(Section 2/3, "what we built")*
**Type:** vertical architecture stack with a clear mTLS trust boundary.
**Content (top→bottom):**
1. **Agent sandbox / laptop** — pyspark client + local egress (holds per-principal client cert `alice`).
2. **‖ mTLS ‖** → internal **NLB (mTLS :15009)**.
3. **Connect pod (namespace `spark`):** **Envoy sidecar** (terminates mTLS, verifies client cert → sets `x-connect-principal`, injects PSK) → **Spark Connect driver** (loopback :15002 only; `PrincipalPinningInterceptor`: reject if `user_id ≠ principal`).
4. **Executor pods** (executor node group) · **HMS (:9083)** catalog · **S3 Iceberg warehouse** (IRSA).
**Annotations:** "no path to raw :15002 (loopback-bound)" · "principal-pinned, fails closed (verified)."
**Caption:** *The agent reaches Spark only through mTLS-fronted Connect; the interceptor pins the Spark user to the cert principal and fails closed. Driver + executors run as pods; storage is Iceberg on S3. This is the governed ingress the control boundary runs over.*
**Design note:** draw the mTLS boundary as the security line; grey the agent (untrusted), blue the control pod.

### D9. Demonstration-layer ladder — proven vs proposed (the drift detector)  *(§11)*
**Type:** vertical ladder L0→L6, each rung a status chip. This is the honest scorecard — great for
"what we've built and what's next."
**Content (bottom→top, status as of 2026-07-06):**
- **L0** substrate (EKS Connect + Envoy + catalog + S3) — ✅ built/ran.
- **L1** native mTLS channel — 🟡 PARTIAL (socat stand-in).
- **L2** off-host execution — ✅ **both arms** (was Arm-A only).
- **L3** remote SDP green — ✅ **DEMONSTRATED (2026-07-06)** ← highlight: this closed today.
- **L4** gate before data — ✅ demonstrated remote.
- **L5** governance split (reconciler off agent host) — ⛔ GAP (the load-bearing remaining work).
- **L6** negative control (imperative can't traverse) — 🟡 refined (DataFrame-API imperative *can*; sparkContext/_jvm can't).
- Marker: "highest honestly-claimable layer today ≈ **L3**."
**Caption:** *What's proven vs proposed. The control boundary is demonstrated across hosts up to L3 (both arms remote, SDP green — closed 2026-07-06); the governance split (L5) is the named remaining gap; Section 3's governed multi-tenant platform stays a proposal.*

---

## TIER 4 — optional (methods / apparatus)

### D10. The run-cell loop + compute measurement  *(§6 apparatus)*
`propose → materialize → [dry-run gate] → execute → grade`, looping to `max_iterations`. Annotate the
**stage-diff** compute measurement (snapshot Spark-UI stages before/after execute → `executor_seconds`)
and the per-attempt serialization into `per_iteration`. For the methods-savvy reader.

### D11. H3 compute — wasted vs total  *(§4.3, HOLD until the larger sweep confirms)*
Bars: H3.1 wasted-compute-on-failed (A burns, B gate-intercepts → ~0) and H3.2 total-compute-to-correct.
**Do not finalize yet** — current numbers are a first directional sweep (N small). Placeholder only.

---

---

## Rendered natives (vision sections — for engineering hand-off)
Two Section 3/4 figures are **already drawn** as clean SVGs (not just specced) so Lisa can circulate
the vision to engineering now. Same visual language as the brief; a status legend on each keeps the
built-vs-vision line honest (**solid = demonstrated · dashed = configured-but-unrun · dotted = frontier**).
Files render standalone and drop into slides/docs; Claude design can refine from these.

- **`diagrams/section3_open_governed_platform.svg`** — the open governed reference architecture: three
  trust zones (U authoring → C control → D data plane), the GitOps loop (agent→PR→dry-run gate→reconciler),
  the single identity-pinned **Connect ingress**, elastic **executor pods (dashed — configured, 0→10 unrun)**,
  and **tenant governance as the dotted frontier** (delegated to catalog + multi-server Connect).
- **`diagrams/section4_omnigent_orchestration.svg`** — Omnigent: one orchestrator/custodian over a
  heterogeneous **credential-free** fleet (`claude_code` / `codex` / `pi`), its four capabilities —
  **model routing (cost)**, **cross-vendor review (quality)**, **credential custody (governance — dotted
  keystone/frontier)**, **shared skill library (knowledge)** — all feeding the §3 platform base.

*Honesty markers baked into the art:* §3 executor elasticity and all of §4's credential-custody binding
read as not-yet-built; only the demonstrated core is solid.

## Suggested figure order in the paper
Abstract/intro: **D1** (thesis). Section 1: **D2** (design) → **D3** (taxonomy) → **D4** (structural-catch) →
**D5** (D7 finding) → **D6** (composition) → **D7** (conciseness). Section 2: **D8** (topology) → **D9** (ladder).
Appendix/methods: **D10**, **D11** (when ready).

**Priorities if only a few get made:** D1, D5, D9 (thesis, the standout finding, the honest scorecard) —
those three alone carry "what we built, what we found, and what's proven."
