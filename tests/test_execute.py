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


def test_check_sandbox_blocks_backtick_quoted_writes():
    with pytest.raises(SandboxViolation):
        check_sandbox("CREATE TABLE `ground_truth`.`gt` AS SELECT 1", "sandbox_p1")


def test_check_sandbox_handles_if_not_exists():
    with pytest.raises(SandboxViolation):
        check_sandbox("CREATE TABLE IF NOT EXISTS ground_truth.evil AS SELECT 1",
                      "sandbox_p1")
    check_sandbox("CREATE TABLE IF NOT EXISTS sandbox_p1.ok AS SELECT 1",
                  "sandbox_p1")


def test_check_sandbox_blocks_insertinto_and_writeto():
    with pytest.raises(SandboxViolation):
        check_sandbox('df.write.insertInto("staging_inputs.x")', "sandbox_p1")
    with pytest.raises(SandboxViolation):
        check_sandbox('df.writeTo("ground_truth.gt").append()', "sandbox_p1")
    check_sandbox('df.write.insertInto("sandbox_p1.ok")', "sandbox_p1")


def test_check_sandbox_blocks_update_delete_alter_replace():
    for stmt in ("UPDATE ground_truth.gt SET x = 1",
                 "DELETE FROM ground_truth.gt WHERE x = 1",
                 "ALTER TABLE ground_truth.gt ADD COLUMN y INT",
                 "REPLACE TABLE ground_truth.gt AS SELECT 1"):
        with pytest.raises(SandboxViolation):
            check_sandbox(stmt, "sandbox_p1")
    check_sandbox("UPDATE staging SET x = 1", "sandbox_p1")  # unqualified ok
    check_sandbox("MERGE INTO sandbox_p1.t USING s ON t.k=s.k "
                  "WHEN MATCHED THEN UPDATE SET x = 1", "sandbox_p1")


def test_check_sandbox_blocks_schema_catalog_context_statements():
    for stmt in ('USE staging_inputs',
                 'DROP SCHEMA ground_truth CASCADE',
                 'CREATE SCHEMA evil',
                 'spark.catalog.setCurrentDatabase("staging_inputs")',
                 "INSERT OVERWRITE DIRECTORY '/tmp/x' SELECT 1"):
        with pytest.raises(SandboxViolation):
            check_sandbox(stmt, "sandbox_p1")


def test_check_sandbox_still_allows_existing_cases():
    check_sandbox("CREATE TABLE sandbox_p1.x AS SELECT 1", "sandbox_p1")
    check_sandbox("SELECT * FROM ground_truth.gt", "sandbox_p1")


def test_check_sandbox_blocks_prefix_string_literals():
    with pytest.raises(SandboxViolation):
        check_sandbox('df.write.saveAsTable(f"ground_truth.x")', "sandbox_p1")


def test_check_sandbox_normalizes_comments_and_spaced_dots():
    with pytest.raises(SandboxViolation):
        check_sandbox("CREATE TABLE -- note\nground_truth.x AS SELECT 1",
                      "sandbox_p1")
    with pytest.raises(SandboxViolation):
        check_sandbox("DROP TABLE ground_truth . gt", "sandbox_p1")


def test_check_sandbox_in_string_comment_does_not_mask_write():
    with pytest.raises(SandboxViolation):
        check_sandbox("SELECT 'a--b' AS x; DROP TABLE ground_truth.gt;",
                      "sandbox_p1")


def test_check_sandbox_bracketed_comment_between_keyword_and_target():
    with pytest.raises(SandboxViolation):
        check_sandbox("CREATE TABLE /* note */ ground_truth.x AS SELECT 1",
                      "sandbox_p1")


def test_split_sql_handles_escaped_quotes():
    stmts = split_sql("SELECT 'O''Brien; x' AS n; SELECT 2;")
    assert len(stmts) == 2
    assert "O''Brien; x" in stmts[0]


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
