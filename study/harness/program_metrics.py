"""Conciseness metrics on the agent's FINAL accepted program (hypothesis H5).

The study's headline qualitative claim is that a DECLARATIVE agent (Arm B, SDP)
owns a SMALLER decision surface than an IMPERATIVE one (Arm A2, the paradigm-
matched control with the SAME gate + skill): it writes less code to do the same
job. This module makes that measurable on the program the agent actually shipped
-- the source that reached a COMPLETED output (captured per cell by the runner) --
so the B-vs-A2 conciseness contrast in `analysis/analyze.py` can compute it over N.

TWO metrics, each reported BOTH **raw** and **transform-body-only**:

  * `final_program_loc`      -- non-blank, non-comment source lines (comment/blank
                                detection via Python's `tokenize`, so a `#` inside a
                                string/docstring is NOT mistaken for a comment).
  * `ast_node_count`         -- total `ast` nodes (every node `ast.walk` yields,
                                including the `Module` root and the `Load`/`Store`
                                contexts and operator nodes -- a consistent,
                                paradigm-agnostic structural size).

The **body-only** variants exclude the MANDATORY, DECISION-FREE scaffolding a
paradigm forces on the agent but that carries no task decision:

  * `import` / `from ... import` statements,
  * the `def`/`class` HEADER (signature line(s) through the colon) of every
    function/class,
  * **BARE** structural decorators ONLY -- a decorator with NO arguments
    (`@dp.table`, `@dp.materialized_view`, `@dp.view`, `@dp.table()`): a
    decision-free wrapper.

CRITICAL (cross-review fix): a decorator that CARRIES ARGUMENTS is **logic**, not
scaffolding, and is COUNTED in body-only. In Spark Declarative Pipelines a `@dp`
decorator routinely encodes AGENT DECISIONS -- data-quality expectations
(`@dp.expect(...)` / `expect_all` / `expect_or_drop`), `table_properties`,
`partition_cols` / `cluster_by`, schema hints, the chosen `name`/`comment`, etc.
Stripping every decorator would omit those DECLARATIVE decisions from the body-only
metric while the equivalent IMPERATIVE quality logic (a `.filter(...)`, a
`partitionBy(...)`) stays counted in the imperative body -- quietly biasing the
metric toward the very hypothesis under test. So the predicate is:

    decorator with ZERO args/keywords  -> scaffolding (strippable)
    decorator with ANY  args/keywords  -> logic-bearing (counted, whole expression)

The predicate is applied IDENTICALLY in both paradigms -- it is NOT special-cased by
arm or by decorator name; the SAME rule decides scaffolding-vs-logic for any
decorator in either arm. So an imperative program with equivalent inline quality
logic and a declarative program with an `@dp.expect(...)` both keep that logic.

The function/class BODY statements are always KEPT. Net effect: declarative is not
penalised for the bare `@dp` wrapper + `def` it is REQUIRED to write, while every
genuine decision -- declarative-in-a-decorator or imperative-inline -- is counted on
both sides.

WHAT IS MEASURED (the `.yml` decision -- documented per the H5 spec):
The captured artifact is the agent-AUTHORED program ONLY -- `proposal.code`, i.e.
the imperative `pipeline.py` or the SDP `transformations/pipeline.py` @dp module.
The SDP project's `spark-pipeline.yml` is **EXCLUDED** from both metrics. Rationale:
that spec is HARNESS boilerplate (`runner._sdp_spec` emits it from the study config;
it holds catalog/database/storage/glob and NO agent logic -- see the no-leak guard
in `tests/test_workspace_contract.py`). The agent never writes it, so counting it
would attribute harness-authored YAML to the declarative agent and INFLATE its
measured surface against the very claim under test. Conciseness is therefore an
apples-to-apples comparison of the two agent-authored Python programs.

NULLABILITY ON PARSE FAILURE (explicit): an empty/None program -> ALL four fields
None. A program that fails to `ast.parse` (a SyntaxError) keeps the lexical/`tokenize`
raw `final_program_loc` (still meaningful) but sets the THREE parse-dependent fields
(`final_program_loc_body`, `ast_node_count`, `ast_node_count_body`) to None -- "some
fields null on parse failure", NOT "all fields null".

No third-party dependencies (stdlib `ast` + `tokenize` only) so it imports anywhere
the schema and runner do.
"""
from __future__ import annotations

import ast
import io
import tokenize
from typing import Dict, Optional, Set


# ---------------------------------------------------------------------------
# LOC (non-blank, non-comment source lines -- tokenize-based, string-aware)
# ---------------------------------------------------------------------------
# Token types that do NOT make a line "code": comments, the various newline/layout
# tokens, and the stream sentinels. Everything else (NAME/OP/NUMBER/STRING/FSTRING/...)
# is real code on its line(s).
_NON_CODE_TOKENS = frozenset(
    t for t in (
        getattr(tokenize, "COMMENT", None), getattr(tokenize, "NL", None),
        getattr(tokenize, "NEWLINE", None), getattr(tokenize, "INDENT", None),
        getattr(tokenize, "DEDENT", None), getattr(tokenize, "ENCODING", None),
        getattr(tokenize, "ENDMARKER", None),
    ) if t is not None
)


def _code_lines_lexical(source: str) -> Set[int]:
    """Dependency-free fallback used only when `tokenize` cannot scan the source: a
    line counts unless it is blank or its first non-whitespace char is `#`."""
    out: Set[int] = set()
    for i, line in enumerate(source.splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.add(i)
    return out


def _code_line_numbers(source: str) -> Set[int]:
    """1-based line numbers carrying real code (non-blank, non-comment).

    Uses Python's `tokenize`, so the comment rule is token-aware: ONLY genuine
    COMMENT tokens are excluded. A line INSIDE a triple-quoted string/docstring that
    happens to start with `#` is part of a STRING token and is correctly counted as
    code (the lexical first-char rule got this wrong). A multi-line string marks every
    line it spans as code (it is one statement). Falls back to the lexical rule if
    `tokenize` raises (e.g. an unterminated string)."""
    out: Set[int] = set()
    try:
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type in _NON_CODE_TOKENS:
                continue
            for ln in range(tok.start[0], tok.end[0] + 1):
                out.add(ln)
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return _code_lines_lexical(source)
    return out


# ---------------------------------------------------------------------------
# scaffolding-vs-logic predicate for decorators (paradigm-agnostic)
# ---------------------------------------------------------------------------
def _is_scaffolding_decorator(dec: ast.expr) -> bool:
    """True iff `dec` is a BARE structural wrapper carrying NO agent decision:

      * a name or attribute with no call           -> `@dp.table`, `@a.b.c`
      * a call with NO positional and NO keyword    -> `@dp.table()`

    A decorator with ANY argument (positional or keyword) encodes a decision
    (expectation/constraint/property/partition/schema/name/...) and is LOGIC, so it
    is NOT scaffolding and stays counted in the body-only metrics. This is the SAME
    test for every decorator in either paradigm -- never special-cased by name/arm."""
    if isinstance(dec, ast.Call):
        return not dec.args and not dec.keywords
    # a non-call decorator (Name/Attribute, or anything else without a call) carries
    # no arguments -> a bare wrapper.
    return isinstance(dec, (ast.Name, ast.Attribute))


# ---------------------------------------------------------------------------
# scaffolding line/node sets (imports + bare decorators + def/class headers)
# ---------------------------------------------------------------------------
def _scaffolding_line_numbers(tree: ast.AST) -> Set[int]:
    """Line numbers occupied by mandatory, decision-free scaffolding: import
    statements, BARE structural decorators (logic-bearing decorators are kept), and
    function/class HEADER lines (signature through the colon)."""
    lines: Set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for dec in node.decorator_list:
                if _is_scaffolding_decorator(dec):
                    lines.update(range(dec.lineno, (dec.end_lineno or dec.lineno) + 1))
            # header = the `def`/`class` line through the line before the first body
            # statement (captures multi-line signatures). node.lineno is the def/class
            # line itself (py3.8+), NOT a decorator line.
            first_body = node.body[0].lineno if node.body else node.lineno
            lines.update(range(node.lineno, first_body))
    return lines


def _scaffolding_nodes(tree: ast.AST) -> Set[int]:
    """ids() of AST nodes that are decision-free scaffolding: whole import subtrees,
    every function/class def node itself + its signature subtree + its BARE decorator
    subtrees (logic-bearing decorators are KEPT, body is KEPT)."""
    excluded: Set[int] = set()

    def _add_subtree(n: ast.AST) -> None:
        for d in ast.walk(n):
            excluded.add(id(d))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            _add_subtree(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            excluded.add(id(node))                 # the def/class wrapper node itself
            for dec in node.decorator_list:        # bare @dp.table etc. -> strip;
                if _is_scaffolding_decorator(dec):  # @dp.expect(...) etc. -> keep (logic)
                    _add_subtree(dec)
            args = getattr(node, "args", None)     # the signature (FunctionDef)
            if args is not None:
                _add_subtree(args)
            returns = getattr(node, "returns", None)
            if returns is not None:
                _add_subtree(returns)
            for base in getattr(node, "bases", []):       # ClassDef bases ...
                _add_subtree(base)
            for kw in getattr(node, "keywords", []):      # ... and metaclass kwargs
                _add_subtree(kw)
    return excluded


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------
def program_metrics(source: Optional[str]) -> Dict[str, Optional[int]]:
    """Compute the four conciseness numbers for one agent program.

    Returns a dict with `final_program_loc`, `final_program_loc_body`,
    `ast_node_count`, `ast_node_count_body`. An empty/None source -> all None; a
    source that fails to parse keeps the raw LOC but sets every AST-derived field
    (and body LOC, which needs the parse) to None. The metric is paradigm-agnostic --
    imperative and declarative programs are measured by the SAME rule, and the
    scaffolding-vs-logic decorator predicate is applied identically in both arms.
    """
    empty = {
        "final_program_loc": None,
        "final_program_loc_body": None,
        "ast_node_count": None,
        "ast_node_count_body": None,
    }
    if not source or not source.strip():
        return dict(empty)

    code_lines = _code_line_numbers(source)
    out: Dict[str, Optional[int]] = dict(empty)
    out["final_program_loc"] = len(code_lines)

    try:
        tree = ast.parse(source)
    except SyntaxError:
        # raw LOC is still meaningful; AST-derived fields are not computable.
        return out

    scaffold_lines = _scaffolding_line_numbers(tree)
    out["final_program_loc_body"] = len(code_lines - scaffold_lines)

    all_nodes = list(ast.walk(tree))
    out["ast_node_count"] = len(all_nodes)
    excluded = _scaffolding_nodes(tree)
    out["ast_node_count_body"] = sum(1 for n in all_nodes if id(n) not in excluded)
    return out


# the canonical field order, reused by the schema + analysis so they never drift.
CONCISENESS_FIELDS = (
    "final_program_loc",
    "final_program_loc_body",
    "ast_node_count",
    "ast_node_count_body",
)
