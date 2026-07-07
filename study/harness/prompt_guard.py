"""Shared prompt-no-leak vocabulary + checker (corpus v3 §1).

This is the single source of truth for the banned how-to / API / encoding / defect
vocabulary that must never appear in the AGENT-VISIBLE prompt wiring the harness
composes (the ticket, the paradigm instructions, and the input-location lines).
It lives in the importable `harness` package so EVERY test can use it the same way
(`from harness.prompt_guard import leaks`), instead of a fragile cross-test import
that breaks under pytest's per-file module layout.

Symptoms are allowed (duplicate, late, missing, malformed, foreign currency, wrong
totals, double-counting). What is banned is the *solution* vocabulary: the Spark
API symbol, the named fix/technique, the encoding hint, the defect-class name --
and any input-read how-to ('read each as ...'), since the multi-input contract must
reveal ONLY input name + location, never a format or how-to hint.
"""
from __future__ import annotations

import re
from typing import List

# (a) Spark / PySpark API symbols the agent must discover, not be handed.
API_TOKENS = [
    "withwatermark", "dropduplicat", "from_json", "to_utc_timestamp", "to_date(",
    "to_timestamp", "row_number", "partitionby", "groupby", "apply_changes",
    "spark.conf", "create_map", "window(", ".cast(", "merge into", "createorreplace",
]
# (b) named fixes / techniques (the "how", not the "what").
FIX_TOKENS = [
    "watermark", "deduplicat", "dedup", "coerce", "quarantine", "dead-letter", "dlq",
    "upsert", "idempoten", "as-of", "as of", "scd type", "sessioniz",
    "tombstone", "effective_from", "effective_to", "left join", "left-join",
    "inner join", "window function", "watermarked",
]
# (c) encoding hints that prescribe the exact trap.
ENCODING_TOKENS = [
    "epoch", "millis", "iso-8601", "iso8601", "session-local", "timezone",
]
# (d) defect-class / instrument vocabulary.
DEFECT_TOKENS = [
    "silent defect", "non-deterministic", "nondeterministic", "arbitrary survivor",
    "unbounded", "misparse", "mis-bucket", "misbucket", "deterministic survivor",
    "deterministic ordering", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9",
]
# (e) input-read how-to hints. The multi-input aux contract must reveal ONLY name +
# location; an INSTRUCTION like "read each as NDJSON" / "read it as ..." is a how-to
# leak. We ban the how-to VERB phrase, not the bare format label -- the primary
# 'Dataset (NDJSON, this seed): <path>' line legitimately LABELS the format without
# telling the agent how to read it, and stays the accepted boundary. Word-boundary
# matched so they cannot false-positive on "read the".
READ_HOWTO_TOKENS = [
    "read each", "read as", "read it as", "read them as", "read each as",
]
BANNED = API_TOKENS + FIX_TOKENS + ENCODING_TOKENS + DEFECT_TOKENS + READ_HOWTO_TOKENS


# short natural-language phrases that need WORD-BOUNDARY matching to avoid false
# positives (e.g. "was officially" -> "as of"; "Brasilia" -> n/a). API symbols and
# prefix tokens stay substring-matched.
BOUNDARY = {"as of", "as-of", "scd type", "left join", "left-join", "inner join",
            "window function", "dlq", "deterministic ordering",
            "read each", "read as", "read it as", "read them as", "read each as"}


def leaks(text: str) -> List[str]:
    """Return the banned tokens present in `text` (empty list == clean)."""
    low = text.lower()
    hits = []
    for tok in BANNED:
        if re.fullmatch(r"d[1-9]", tok):
            # D1..D9 only count as a leak when standalone (not inside a word/number)
            if re.search(rf"(?<![\w]){tok}(?![\w0-9])", low):
                hits.append(tok)
        elif tok in BOUNDARY:
            if re.search(rf"\b{re.escape(tok)}\b", low):
                hits.append(tok)
        elif tok in low:
            hits.append(tok)
    return hits
