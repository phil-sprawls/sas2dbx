"""Deterministic preprocessing — no SAS parser. Resolve %include (file I/O the
LLM cannot do), expand simple %let variables (mechanical bookkeeping the LLM is
bad at), split at DATA/PROC boundaries (step-scoped LLM calls raise accuracy
and make repair attributable). Complex %macro bodies pass through whole."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

INCLUDE_RE = re.compile(r"%include\s+['\"]([^'\"]+)['\"]\s*;", re.IGNORECASE)
LET_RE = re.compile(r"^\s*%let\s+(\w+)\s*=\s*([^;]*);\s*$", re.IGNORECASE | re.MULTILINE)
STEP_START_RE = re.compile(r"^\s*(data\b|proc\s+\w+|%macro\b)", re.IGNORECASE)
STEP_END_RE = re.compile(r"^\s*(run|quit)\s*;", re.IGNORECASE)
MACRO_END_RE = re.compile(r"^\s*%mend\b.*;", re.IGNORECASE)


@dataclass
class SasStep:
    index: int
    kind: str   # global | data | proc | macro
    code: str


def resolve_includes(source: str, base_dir: str, max_depth: int = 10) -> str:
    if max_depth <= 0:
        raise RecursionError("%include nesting exceeds max depth")

    def repl(m: re.Match) -> str:
        path = m.group(1)
        if not os.path.isabs(path):
            path = os.path.join(base_dir, path)
        with open(path) as f:
            return resolve_includes(f.read(), os.path.dirname(path), max_depth - 1)

    return INCLUDE_RE.sub(repl, source)


def expand_lets(source: str) -> str:
    lets = {name: value.strip() for name, value in LET_RE.findall(source)}
    out = LET_RE.sub("", source)
    # Longest name first so &prefix doesn't clobber &prefixlonger.
    for name in sorted(lets, key=len, reverse=True):
        out = re.sub(rf"&{re.escape(name)}\.?", lets[name].replace("\\", "\\\\"),
                     out, flags=re.IGNORECASE)
    return out


def split_steps(source: str) -> list[SasStep]:
    steps: list[SasStep] = []
    current: list[str] = []
    kind = "global"

    def flush():
        nonlocal current, kind
        code = "\n".join(current).strip()
        if code:
            steps.append(SasStep(index=len(steps), kind=kind, code=code))
        current, kind = [], "global"

    for line in source.splitlines():
        start = STEP_START_RE.match(line)
        if start and kind == "global":
            flush()
            token = start.group(1).lower()
            kind = "macro" if token.startswith("%macro") else \
                   "data" if token.startswith("data") else "proc"
        current.append(line)
        if kind == "macro" and MACRO_END_RE.match(line):
            flush()
        elif kind in ("data", "proc") and STEP_END_RE.match(line):
            flush()
    flush()
    return steps


def preprocess(path: str) -> tuple[list[SasStep], str]:
    with open(path) as f:
        source = f.read()
    expanded = expand_lets(resolve_includes(source, os.path.dirname(path)))
    return split_steps(expanded), expanded
