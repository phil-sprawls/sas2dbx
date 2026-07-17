"""Parity validation — the trust anchor. Pure comparison logic here; Spark
comparison driver appended in Task 10."""
from __future__ import annotations

import math
from dataclasses import dataclass, field


def values_match(a, b, rel_tol: float = 1e-9) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        if math.isnan(fa) and math.isnan(fb):
            return True
        return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=1e-12)
    return a == b


@dataclass
class ColumnDiff:
    column: str
    mismatches: int
    total: int
    samples: list[str] = field(default_factory=list)

    @property
    def rate(self) -> float:
        return self.mismatches / self.total if self.total else 0.0


@dataclass
class TableDiff:
    table: str
    gt_rows: int
    out_rows: int
    missing_rows: int   # in ground truth, absent from output
    extra_rows: int     # in output, absent from ground truth
    column_diffs: list[ColumnDiff] = field(default_factory=list)
    error: str | None = None

    @property
    def passed(self) -> bool:
        return (self.error is None and self.gt_rows == self.out_rows
                and self.missing_rows == 0 and self.extra_rows == 0
                and all(c.mismatches == 0 for c in self.column_diffs))


@dataclass
class DiffReport:
    program_id: str
    table_diffs: list[TableDiff] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.table_diffs) and all(t.passed for t in self.table_diffs)

    def to_text(self, max_chars: int = 6000) -> str:
        lines: list[str] = []
        for t in self.table_diffs:
            status = "PASS" if t.passed else "FAIL"
            lines.append(f"[{status}] table {t.table}: ground_truth={t.gt_rows} rows, "
                         f"output={t.out_rows} rows, missing={t.missing_rows}, "
                         f"extra={t.extra_rows}")
            if t.error:
                lines.append(f"  error: {t.error}")
            for c in t.column_diffs:
                if c.mismatches:
                    lines.append(f"  column {c.column}: {c.mismatches}/{c.total} "
                                 f"mismatched ({c.rate:.2%})")
                    lines.extend(f"    sample: {s}" for s in c.samples[:5])
        text = "\n".join(lines) or "(no tables compared)"
        if len(text) > max_chars:
            text = text[:max_chars] + "\n...(truncated)"
        return text
