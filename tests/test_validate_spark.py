import pytest

from sas_migrate.validate import compare_tables, validate_program


@pytest.fixture()
def base_tables(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS vt")
    spark.sql("DROP TABLE IF EXISTS vt.gt")
    spark.createDataFrame(
        [(1, "east", 100.0), (2, "west", 200.5), (3, "east", 300.25)],
        "id INT, region STRING, balance DOUBLE").write.saveAsTable("vt.gt")
    yield
    for t in ("gt", "out"):
        spark.sql(f"DROP TABLE IF EXISTS vt.{t}")


def _write_out(spark, rows):
    spark.sql("DROP TABLE IF EXISTS vt.out")
    spark.createDataFrame(rows, "id INT, region STRING, balance DOUBLE") \
         .write.saveAsTable("vt.out")


def test_identical_tables_pass_keyed_and_keyless(spark, base_tables):
    _write_out(spark, [(3, "east", 300.25), (1, "east", 100.0), (2, "west", 200.5)])
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"]).passed
    assert compare_tables(spark, "vt.gt", "vt.out", keys=None).passed  # order-insensitive


def test_float_within_tolerance_passes_keyed(spark, base_tables):
    # Perturb by 1e-5: relative error 1e-7 — outside 1e-9 tolerance, inside 1e-6.
    _write_out(spark, [(1, "east", 100.0 + 1e-5), (2, "west", 200.5), (3, "east", 300.25)])
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"], rel_tol=1e-9).passed is False
    assert compare_tables(spark, "vt.gt", "vt.out", keys=["id"], rel_tol=1e-6).passed


def test_corruption_dropped_row_is_caught(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 200.5)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=["id"])
    assert not d.passed and d.missing_rows == 1


def test_corruption_perturbed_float_is_caught_with_sample(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 999.9), (3, "east", 300.25)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=["id"])
    col = next(c for c in d.column_diffs if c.column == "balance")
    assert col.mismatches == 1
    assert any("id=2" in s for s in col.samples)


def test_corruption_swapped_value_is_caught_keyless(spark, base_tables):
    _write_out(spark, [(1, "west", 100.0), (2, "east", 200.5), (3, "east", 300.25)])
    d = compare_tables(spark, "vt.gt", "vt.out", keys=None)
    assert not d.passed and d.missing_rows == 2 and d.extra_rows == 2


def test_schema_mismatch_reports_error(spark, base_tables):
    spark.sql("DROP TABLE IF EXISTS vt.out")
    spark.createDataFrame([(1, "east")], "id INT, region STRING") \
         .write.saveAsTable("vt.out")
    d = compare_tables(spark, "vt.gt", "vt.out")
    assert d.error and "balance" in d.error


def test_validate_program_aggregates(spark, base_tables):
    _write_out(spark, [(1, "east", 100.0), (2, "west", 200.5), (3, "east", 300.25)])
    report = validate_program(spark, "p1", [("vt.gt", "vt.out", ["id"])])
    assert report.passed and report.program_id == "p1"
