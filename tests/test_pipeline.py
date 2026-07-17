import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.gateway import MockGateway, TokenBudget
from sas_migrate.inventory import Inventory, ProgramRecord
from sas_migrate.pipeline import PipelineDeps, build_table_schemas, migrate_program
from sas_migrate.report import Reporter
from sas_migrate.statestore import LocalJsonStateStore
from sas_migrate.translate import Translator


SAS_PROGRAM = """\
data work.filtered;
  set staging.customers;
  where region = 'east';
run;
"""

GOOD_RESPONSE = """\
```json
{"language": "sql", "inputs": ["staging_pl.customers"], "outputs": ["sandbox_pl1.filtered"]}
```
```sql
CREATE TABLE sandbox_pl1.filtered AS
SELECT * FROM staging_pl.customers WHERE region = 'east';
```"""

BAD_THEN_FIXED = """\
```json
{"language": "sql", "inputs": ["staging_pl.customers"], "outputs": ["sandbox_pl1.filtered"]}
```
```sql
CREATE TABLE sandbox_pl1.filtered AS
SELECT * FROM staging_pl.customers WHERE region = 'west';
```"""


@pytest.fixture()
def env(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS staging_pl")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gt_pl")
    spark.sql("DROP TABLE IF EXISTS staging_pl.customers")
    spark.sql("DROP TABLE IF EXISTS gt_pl.filtered")
    spark.createDataFrame(
        [(1, "east", 10.0), (2, "west", 20.0), (3, "east", 30.0)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("staging_pl.customers")
    spark.createDataFrame(
        [(1, "east", 10.0), (3, "east", 30.0)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("gt_pl.filtered")
    sas = tmp_path / "pl1.sas"
    sas.write_text(SAS_PROGRAM)
    store = LocalJsonStateStore(str(tmp_path / "state"))
    rec = ProgramRecord("pl1", str(sas), "phil",
                        inputs={"staging.customers": "staging_pl.customers"},
                        ground_truth={"filtered": "gt_pl.filtered"})
    yield spark, store, rec
    spark.sql("DROP SCHEMA IF EXISTS sandbox_pl1 CASCADE")


def _deps(store, responses):
    cfg = MigrationConfig()
    translator = Translator(MockGateway(responses), cfg,
                            TokenBudget(cfg.per_program_token_cap))
    inv = Inventory(store)
    return PipelineDeps(config=cfg, inventory=inv, translator=translator,
                        reporter=Reporter(store, cfg), store=store)


def test_build_table_schemas(spark, env):
    schemas = build_table_schemas(spark, ["staging_pl.customers", "nope.missing"])
    assert "region" in schemas["staging_pl.customers"].lower()
    assert "nope.missing" not in schemas


def test_migrate_program_reaches_parity_first_try(env):
    spark, store, rec = env
    deps = _deps(store, [GOOD_RESPONSE])
    deps.inventory.register(rec)
    outcome = migrate_program(spark, rec, deps)
    assert outcome.status == "parity_pass"
    assert deps.inventory.get("pl1").status == "parity_pass"
    assert store.scan("parity_results")[0]["status"] == "parity_pass"


def test_migrate_program_repairs_divergence_then_passes(env):
    spark, store, rec = env
    deps = _deps(store, [BAD_THEN_FIXED, GOOD_RESPONSE])  # wrong filter, then fixed
    deps.inventory.register(rec)
    outcome = migrate_program(spark, rec, deps)
    assert outcome.status == "parity_pass"
    assert outcome.outer_attempts == 2
