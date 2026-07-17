from sas_migrate.preprocess import SasStep
from sas_migrate.translate import (
    CRIBSHEET, SYSTEM_PROMPT, build_match_repair_prompt,
    build_run_repair_prompt, build_translation_prompt,
)


def _prompt_text(messages):
    return "\n".join(m["content"] for m in messages)


def test_translation_prompt_contains_step_context_and_rules():
    step = SasStep(index=2, kind="proc", code="proc sql;\ncreate table work.s as select 1;\nquit;")
    messages = build_translation_prompt(
        step, full_program="/* whole program here */",
        table_schemas={"staging_inputs.customers_ab12": "id BIGINT, region STRING"},
        input_mappings={"mylib.customers": "staging_inputs.customers_ab12"},
        sandbox_schema="sandbox_p1")
    text = _prompt_text(messages)
    assert "create table work.s" in text          # the step being translated
    assert "/* whole program here */" in text      # full program as reference
    assert "id BIGINT, region STRING" in text      # live schemas
    assert "mylib.customers" in text               # libref mapping
    assert "sandbox_p1" in text                    # write target rule
    assert "1960" in CRIBSHEET                     # date epoch gotcha present
    assert "Spark SQL" in SYSTEM_PROMPT


def test_run_repair_prompt_includes_traceback():
    messages = build_run_repair_prompt("SELECT bad", "sql", "AnalysisException: bad")
    text = _prompt_text(messages)
    assert "AnalysisException" in text and "SELECT bad" in text


def test_match_repair_prompt_includes_diff():
    messages = build_match_repair_prompt("SELECT 1", "sql", "col n: 4 mismatches (0.4%)")
    text = _prompt_text(messages)
    assert "4 mismatches" in text and "SELECT 1" in text
