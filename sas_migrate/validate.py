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


FLOAT_TYPES = ("double", "float", "decimal")


def _columns(spark, table: str) -> dict[str, str]:
    return {f.name.lower(): f.dataType.simpleString()
            for f in spark.table(table).schema.fields}


def _is_float(dtype: str) -> bool:
    return any(dtype.startswith(t) for t in FLOAT_TYPES)


def _hash_expr(cols: dict[str, str]) -> str:
    parts = []
    for name, dtype in sorted(cols.items()):
        if _is_float(dtype):
            parts.append(f"coalesce(format_string('%.9e', `{name}`), '\\x00')")
        else:
            parts.append(f"coalesce(cast(`{name}` AS STRING), '\\x00')")
    return f"sha2(concat_ws('\\x1f', {', '.join(parts)}), 256)"


def compare_tables(spark, gt_table: str, out_table: str,
                   keys: list[str] | None = None, rel_tol: float = 1e-9,
                   sample_limit: int = 5) -> TableDiff:
    gt_cols, out_cols = _columns(spark, gt_table), _columns(spark, out_table)
    if set(gt_cols) != set(out_cols):
        only_gt = sorted(set(gt_cols) - set(out_cols))
        only_out = sorted(set(out_cols) - set(gt_cols))
        return TableDiff(out_table, 0, 0, 0, 0, error=(
            f"schema mismatch: ground-truth-only columns {only_gt}, "
            f"output-only columns {only_out}"))
    gt_rows = spark.table(gt_table).count()
    out_rows = spark.table(out_table).count()

    if keys:
        return _compare_keyed(spark, gt_table, out_table, gt_cols, keys,
                              rel_tol, sample_limit, gt_rows, out_rows)
    return _compare_keyless(spark, gt_table, out_table, gt_cols,
                            gt_rows, out_rows)


def _compare_keyed(spark, gt_table, out_table, cols, keys, rel_tol,
                   sample_limit, gt_rows, out_rows) -> TableDiff:
    keys = [k.lower() for k in keys]
    on = " AND ".join(f"g.`{k}` <=> o.`{k}`" for k in keys)
    joined = f"(SELECT * FROM {gt_table}) g FULL OUTER JOIN (SELECT * FROM {out_table}) o ON {on}"
    anchor = keys[0]
    missing = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE o.`{anchor}` IS NULL "
                        f"AND g.`{anchor}` IS NOT NULL").collect()[0]["n"]
    extra = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE g.`{anchor}` IS NULL "
                      f"AND o.`{anchor}` IS NOT NULL").collect()[0]["n"]

    column_diffs: list[ColumnDiff] = []
    matched_filter = f"g.`{anchor}` IS NOT NULL AND o.`{anchor}` IS NOT NULL"
    total = spark.sql(f"SELECT count(*) AS n FROM {joined} WHERE {matched_filter}") \
                 .collect()[0]["n"]
    for name, dtype in sorted(cols.items()):
        if name in keys:
            continue
        if _is_float(dtype):
            mismatch = (f"NOT (g.`{name}` <=> o.`{name}`) AND NOT ("
                        f"g.`{name}` IS NOT NULL AND o.`{name}` IS NOT NULL AND "
                        f"abs(g.`{name}` - o.`{name}`) <= "
                        f"greatest(abs(g.`{name}`), abs(o.`{name}`)) * {rel_tol} + 1e-12)")
        else:
            mismatch = f"NOT (g.`{name}` <=> o.`{name}`)"
        rows = spark.sql(
            f"SELECT g.`{anchor}` AS k, g.`{name}` AS gv, o.`{name}` AS ov "
            f"FROM {joined} WHERE {matched_filter} AND ({mismatch}) "
            f"LIMIT {sample_limit + 1}").collect()
        if rows:
            count = spark.sql(f"SELECT count(*) AS n FROM {joined} "
                              f"WHERE {matched_filter} AND ({mismatch})").collect()[0]["n"]
            samples = [f"{anchor}={r['k']}: {name} {r['gv']} != {r['ov']}"
                       for r in rows[:sample_limit]]
            column_diffs.append(ColumnDiff(name, count, total, samples))
        else:
            column_diffs.append(ColumnDiff(name, 0, total))
    return TableDiff(out_table, gt_rows, out_rows, missing, extra, column_diffs)


def _compare_keyless(spark, gt_table, out_table, cols, gt_rows, out_rows) -> TableDiff:
    """Multiset diff on row hashes. Float columns are normalized to 10
    significant digits (%.9e) before hashing — the documented tolerance proxy
    for keyless comparison; keyed comparison uses true relative tolerance."""
    h = _hash_expr(cols)
    diff = spark.sql(f"""
        WITH g AS (SELECT {h} AS h, count(*) AS n FROM {gt_table} GROUP BY 1),
             o AS (SELECT {h} AS h, count(*) AS n FROM {out_table} GROUP BY 1)
        SELECT coalesce(g.n, 0) AS gn, coalesce(o.n, 0) AS onn
        FROM g FULL OUTER JOIN o ON g.h = o.h
        WHERE coalesce(g.n, 0) != coalesce(o.n, 0)""").collect()
    missing = sum(max(r["gn"] - r["onn"], 0) for r in diff)
    extra = sum(max(r["onn"] - r["gn"], 0) for r in diff)
    return TableDiff(out_table, gt_rows, out_rows, missing, extra)


def validate_program(spark, program_id: str,
                     table_pairs: list[tuple[str, str, list[str] | None]],
                     rel_tol: float = 1e-9) -> DiffReport:
    return DiffReport(program_id, [
        compare_tables(spark, gt, out, keys=keys, rel_tol=rel_tol)
        for gt, out, keys in table_pairs])
