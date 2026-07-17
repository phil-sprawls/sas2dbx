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
                             sandbox_schema: str,
                             expected_outputs: list[str] | None = None) -> list[dict]:
    mappings = "\n".join(f"- SAS `{k}` -> `{v}`" for k, v in sorted(input_mappings.items())) \
               or "(none)"
    expected_block = ""
    if expected_outputs:
        names = "\n".join(f"- `{name}`" for name in expected_outputs)
        expected_block = f"""

## Required final output tables (write EXACTLY these names)
{names}"""
    content = f"""\
Translate STEP {step.index} (kind={step.kind}) of the SAS program below.

## SAS libref -> catalog table mappings
{mappings}

## Schemas of input tables (tables created by earlier steps are not listed; refer to the full program)
{_schemas_block(table_schemas)}

## Sandbox schema (ALL outputs must be written here)
{sandbox_schema}{expected_block}

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


import json
import re
from dataclasses import dataclass

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import BaseGateway, TokenBudget

JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)```", re.DOTALL)
CODE_BLOCK_RE = re.compile(r"```(sql|python|pyspark)\s*\n(.*?)```", re.DOTALL)


class TranslationParseError(Exception):
    def __init__(self, msg: str, raw: str = ""):
        super().__init__(msg)
        self.raw = raw


@dataclass
class TranslatedStep:
    step_index: int
    language: str            # "sql" | "pyspark"
    code: str
    inputs: list[str]
    outputs: list[str]


def parse_translation_response(text: str, step_index: int) -> TranslatedStep:
    header_m = JSON_BLOCK_RE.search(text)
    if not header_m:
        raise TranslationParseError("missing ```json header block", raw=text)
    try:
        header = json.loads(header_m.group(1))
    except json.JSONDecodeError as e:
        raise TranslationParseError(f"bad json header: {e}", raw=text) from e
    code_m = CODE_BLOCK_RE.search(text, header_m.end())
    if not code_m:
        raise TranslationParseError("missing code block after header", raw=text)
    language = header.get("language", "")
    if language not in ("sql", "pyspark"):
        raise TranslationParseError(f"language must be sql|pyspark, got {language!r}",
                                    raw=text)
    return TranslatedStep(step_index=step_index, language=language,
                          code=code_m.group(2).strip(),
                          inputs=list(header.get("inputs", [])),
                          outputs=list(header.get("outputs", [])))


class Translator:
    def __init__(self, gateway: BaseGateway, config: MigrationConfig,
                 budget: TokenBudget):
        self.gateway = gateway
        self.config = config
        self.budget = budget

    def _call(self, messages: list[dict], step_index: int, purpose: str,
              program_id: str) -> TranslatedStep:
        resp = self.gateway.complete(SYSTEM_PROMPT, messages,
                                     model=self.config.default_model,
                                     purpose=purpose, program_id=program_id)
        self.budget.charge(resp.input_tokens + resp.output_tokens)
        try:
            return parse_translation_response(resp.text, step_index)
        except TranslationParseError as first_err:
            retry = messages + [
                {"role": "assistant", "content": resp.text},
                {"role": "user", "content":
                    f"Your response could not be parsed: {first_err}. Reply again "
                    "with EXACTLY one ```json header block then one code block."}]
            resp2 = self.gateway.complete(SYSTEM_PROMPT, retry,
                                          model=self.config.default_model,
                                          purpose=purpose + "_reparse",
                                          program_id=program_id)
            self.budget.charge(resp2.input_tokens + resp2.output_tokens)
            return parse_translation_response(resp2.text, step_index)

    def translate(self, step, full_program, table_schemas, input_mappings,
                  sandbox_schema, program_id,
                  expected_outputs: list[str] | None = None) -> TranslatedStep:
        messages = build_translation_prompt(step, full_program, table_schemas,
                                            input_mappings, sandbox_schema,
                                            expected_outputs=expected_outputs)
        return self._call(messages, step.index, "translate", program_id)

    def repair_run(self, tstep: TranslatedStep, error_text: str,
                   program_id: str) -> TranslatedStep:
        messages = build_run_repair_prompt(tstep.code, tstep.language, error_text)
        return self._call(messages, tstep.step_index, "repair_run", program_id)

    def repair_match(self, tstep: TranslatedStep, diff_text: str,
                     program_id: str) -> TranslatedStep:
        messages = build_match_repair_prompt(tstep.code, tstep.language, diff_text)
        return self._call(messages, tstep.step_index, "repair_match", program_id)
