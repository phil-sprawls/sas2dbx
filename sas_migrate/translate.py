"""Translation prompts and (Task 8) response parsing + Translator.
Step-scoped calls: the LLM sees the whole program as reference but is asked to
translate exactly one step. Output contract: a ```json header block then one
```sql or ```python code block."""
from __future__ import annotations

from sas_migrate.preprocess import SasStep

CRIBSHEET = """\
SAS -> Spark gotchas you MUST account for:
- SAS dates are days since 1960-01-01 (Spark: days since 1970-01-01). Landed \
tables already store ISO dates; never re-apply epoch offsets.
- SAS missing (.) sorts LOWEST and compares as less-than everything; Spark NULL \
comparisons yield NULL. Rewrite predicates so missing/NULL semantics match SAS.
- DATA step implicit loop with RETAIN keeps values across rows; translate to \
window functions (e.g., last(col, ignoreNulls) over ordered window) or explicit joins.
- FIRST.var / LAST.var -> row_number() over (partition by var order by ...) = 1 \
(or descending for LAST).
- PROC MEANS/SUMMARY default statistics are n, mean, std, min, max; NOPRINT + \
OUTPUT OUT= creates a table that includes _TYPE_ and _FREQ_ columns.
- PROC SORT NODUPKEY keeps the FIRST occurrence -> row_number()=1 pattern, not \
dropDuplicates (which is nondeterministic about which row it keeps).
- SAS character comparisons ignore trailing blanks; landed tables are \
right-trimmed, so use plain equality.
- Numeric/character implicit conversion is automatic in SAS; make every cast \
explicit in Spark SQL.
"""

SYSTEM_PROMPT = """\
You are an expert SAS-to-Databricks migration engineer. Translate one SAS step
at a time into Spark SQL (strongly preferred) or PySpark (only when relational
SQL cannot express the logic, e.g. iterative macro logic or statistical procs).

Rules:
- Emit exactly two fenced blocks: first a ```json header, then one ```sql or
  ```python code block. No prose outside the blocks.
- Header format: {"language": "sql"|"pyspark", "inputs": [tables read],
  "outputs": [tables written]}
- Write output tables ONLY into the sandbox schema you are given. Read inputs
  ONLY from the mapped tables you are given.
- SQL may contain multiple statements separated by semicolons. PySpark code
  receives a `spark` SparkSession variable.
- Preserve SAS semantics exactly; the output will be diffed cell-by-cell
  against SAS ground truth.

""" + CRIBSHEET


def _schemas_block(table_schemas: dict[str, str]) -> str:
    if not table_schemas:
        return "(no table schemas available)"
    return "\n".join(f"- {t}: {s}" for t, s in sorted(table_schemas.items()))


def build_translation_prompt(step: SasStep, full_program: str,
                             table_schemas: dict[str, str],
                             input_mappings: dict[str, str],
                             sandbox_schema: str) -> list[dict]:
    mappings = "\n".join(f"- SAS `{k}` -> `{v}`" for k, v in sorted(input_mappings.items())) \
               or "(none)"
    content = f"""\
Translate STEP {step.index} (kind={step.kind}) of the SAS program below.

## SAS libref -> catalog table mappings
{mappings}

## Live schemas of available tables (including tables created by earlier steps)
{_schemas_block(table_schemas)}

## Sandbox schema (ALL outputs must be written here)
{sandbox_schema}

## Full original program (REFERENCE ONLY — translate only the step)
```sas
{full_program}
```

## The step to translate
```sas
{step.code}
```"""
    return [{"role": "user", "content": content}]


def build_run_repair_prompt(code: str, language: str, error_text: str) -> list[dict]:
    content = f"""\
The following generated {language} code failed to execute. Fix it and return
the same two-block format (```json header then code block). Do not change
which tables it reads or writes.

## Code
```{language}
{code}
```

## Execution error
```
{error_text}
```"""
    return [{"role": "user", "content": content}]


def build_match_repair_prompt(code: str, language: str, diff_text: str) -> list[dict]:
    content = f"""\
The following generated {language} code runs, but its output does NOT match
the SAS ground truth. Analyze the diff, find the semantic divergence from SAS
behavior (check the gotcha list), and return corrected code in the same
two-block format. Do not change which tables it reads or writes.

## Code
```{language}
{code}
```

## Diff vs SAS ground truth
```
{diff_text}
```"""
    return [{"role": "user", "content": content}]
