import pytest

from sas_migrate.config import MigrationConfig
from sas_migrate.execute import Executor, SandboxViolation, check_sandbox, split_sql
from sas_migrate.translate import TranslatedStep


def test_split_sql_respects_quoted_semicolons():
    stmts = split_sql("SELECT 'a;b' AS x; CREATE TABLE t AS SELECT 1;")
    assert len(stmts) == 2
    assert stmts[0] == "SELECT 'a;b' AS x"


def test_check_sandbox_allows_sandbox_and_unqualified_writes():
    check_sandbox("CREATE TABLE sandbox_p1.out AS SELECT 1", "sandbox_p1")
    check_sandbox("CREATE TABLE out AS SELECT 1", "sandbox_p1")
    check_sandbox("SELECT * FROM ground_truth.gt", "sandbox_p1")  # reads are fine


def test_check_sandbox_blocks_writes_outside():
    with pytest.raises(SandboxViolation):
        check_sandbox("DROP TABLE ground_truth.gt", "sandbox_p1")
    with pytest.raises(SandboxViolation):
        check_sandbox('df.write.saveAsTable("staging_inputs.x")', "sandbox_p1")


@pytest.fixture()
def executor(spark):
    ex = Executor(spark, MigrationConfig(), "p1")
    ex.reset()
    yield ex
    spark.sql("DROP SCHEMA IF EXISTS sandbox_p1 CASCADE")


def test_run_sql_step_creates_table(spark, executor):
    t = TranslatedStep(0, "sql",
                       "CREATE TABLE sandbox_p1.a AS SELECT 1 AS x; "
                       "CREATE TABLE sandbox_p1.b AS SELECT x + 1 AS y FROM sandbox_p1.a;",
                       [], ["sandbox_p1.a", "sandbox_p1.b"])
    r = executor.run_step(t)
    assert r.ok and r.duration_s >= 0
    assert spark.table("sandbox_p1.b").collect()[0]["y"] == 2


def test_run_pyspark_step(spark, executor):
    code = 'spark.sql("SELECT 42 AS v").write.saveAsTable("sandbox_p1.pys")'
    r = executor.run_step(TranslatedStep(1, "pyspark", code, [], ["sandbox_p1.pys"]))
    assert r.ok
    assert spark.table("sandbox_p1.pys").collect()[0]["v"] == 42


def test_run_step_captures_error_with_traceback(executor):
    r = executor.run_step(TranslatedStep(2, "sql", "SELECT * FROM nope.missing", [], []))
    assert not r.ok
    assert "nope" in r.error or "TABLE_OR_VIEW_NOT_FOUND" in r.error


def test_reset_clears_sandbox(spark, executor):
    executor.run_step(TranslatedStep(0, "sql",
                      "CREATE TABLE sandbox_p1.tmp AS SELECT 1 AS x", [], []))
    executor.reset()
    assert not spark.catalog.tableExists("sandbox_p1.tmp")
