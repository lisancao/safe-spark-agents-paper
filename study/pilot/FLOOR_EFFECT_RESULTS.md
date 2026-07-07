# Floor-effect pilot results (corpus v3 §0/§11)

Gate: per high-complexity / elevated task and seed, a REFERENCE-CORRECT solution
must PASS the task's deterministic oracle and a REFERENCE-DEFECTIVE solution
(committing the exact silent defect the task targets) must be CAUGHT. A correct
build that passes proves the task is **not impossible** (some arm can win); a
defect that is caught proves there **is signal**. A task is FLAGGED if either
fails. See `floor_effect_pilot.py` and `tests/test_floor_effect_pilot.py`.

NOTE: this is the deterministic offline gate. The agentic both-paradigm loop
(imperative vs SDP/Connect, real agents) needs an ANTHROPIC_API_KEY + a live
Spark Connect cluster, neither available in this environment, and is DEFERRED to
calibration. The reference-correct solutions are paradigm-agnostic PySpark that
run identically on both substrates, so the gate stands in for the floor question.

## Outcomes (seeds 42, 1337)

```
seed    42  HC1_fx_trade_ledger    correct=PASS defective=CAUGHT           signal=True  -- defective superseded-rate mismatches 5 currencies
seed    42  HC2_session_funnel     correct=PASS defective=CAUGHT           signal=True  -- defective drops 11 malformed events with no DLQ
seed    42  p13_cdc_windowed       correct=PASS defective=CAUGHT           signal=True  -- defective keeps 12 tombstoned customers in current state
seed    42  p14_fx_settlement      correct=PASS defective=CAUGHT           signal=True  -- defective flat-FX is off on 3/4 settlement days
seed  1337  HC1_fx_trade_ledger    correct=PASS defective=CAUGHT           signal=True  -- defective superseded-rate mismatches 2 currencies
seed  1337  HC2_session_funnel     correct=PASS defective=CAUGHT           signal=True  -- defective drops 8 malformed events with no DLQ
seed  1337  p13_cdc_windowed       correct=PASS defective=CAUGHT           signal=True  -- defective keeps 16 tombstoned customers in current state
seed  1337  p14_fx_settlement      correct=PASS defective=CAUGHT           signal=True  -- defective flat-FX is off on 3/4 settlement days

FLAGGED (possible floor / no signal): none
{"n_runs": 8, "n_flagged": 0}
```

All four tasks: correct=PASS, defective=CAUGHT, signal=True on both seeds; **no
floors flagged**. The defective levers exercised: HC-1 keeps a superseded FX
revision (no order-by-seq) → per-currency positions diverge; HC-2 silently drops
malformed clicks (no DLQ) → event accounting breaks; p13 omits the tombstone
filter → deleted customers persist in current state; p14 uses a flat FX (ignores
the daily rate) → settlement days diverge from the bank's independent totals.
