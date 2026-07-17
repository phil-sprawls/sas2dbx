import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import MockGateway, TokenBudget
from sas_migrate.preprocess import SasStep
from sas_migrate.translate import (
    TranslationParseError, Translator, parse_translation_response,
)

GOOD = """\
```json
{"language": "sql", "inputs": ["staging_inputs.customers_ab12"], "outputs": ["sandbox_p1.filtered"]}
```
```sql
CREATE TABLE sandbox_p1.filtered AS
SELECT * FROM staging_inputs.customers_ab12 WHERE signup_date >= '2024-01-01';
```"""

GOOD_PYSPARK = """\
```json
{"language": "pyspark", "inputs": [], "outputs": ["sandbox_p1.stats"]}
```
```python
df = spark.table("sandbox_p1.filtered")
df.describe().write.saveAsTable("sandbox_p1.stats")
```"""


def test_parse_good_sql_response():
    t = parse_translation_response(GOOD, step_index=1)
    assert t.language == "sql"
    assert t.outputs == ["sandbox_p1.filtered"]
    assert "CREATE TABLE sandbox_p1.filtered" in t.code
    assert t.step_index == 1


def test_parse_pyspark_response():
    t = parse_translation_response(GOOD_PYSPARK, step_index=2)
    assert t.language == "pyspark"
    assert 'spark.table("sandbox_p1.filtered")' in t.code


def test_parse_missing_header_raises():
    with pytest.raises(TranslationParseError):
        parse_translation_response("```sql\nSELECT 1;\n```", step_index=0)


def _translator(responses):
    return Translator(MockGateway(responses), MigrationConfig(),
                      TokenBudget(500_000))


def test_translator_translate_returns_step():
    tr = _translator([GOOD])
    step = SasStep(index=1, kind="data", code="data f; set c; run;")
    t = tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert t.language == "sql"


def test_translator_retries_parse_failure_once_then_raises():
    tr = _translator(["not parseable", "still not parseable"])
    step = SasStep(index=0, kind="data", code="data a; run;")
    with pytest.raises(TranslationParseError):
        tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert len(tr.gateway.calls) == 2  # retried exactly once


def test_translator_charges_budget():
    tr = _translator([GOOD])
    step = SasStep(index=0, kind="data", code="data a; run;")
    tr.translate(step, "prog", {}, {}, "sandbox_p1", "p1")
    assert tr.budget.used > 0
