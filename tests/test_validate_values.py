from sas_migrate.validate import ColumnDiff, DiffReport, TableDiff, values_match


def test_values_match_nulls_and_exact():
    assert values_match(None, None)
    assert not values_match(None, 0)
    assert values_match("abc", "abc")
    assert not values_match("abc", "abd")
    assert values_match(7, 7)


def test_values_match_float_relative_tolerance():
    assert values_match(1000000.0, 1000000.0000001, rel_tol=1e-9)   # within
    assert not values_match(1000000.0, 1000000.01, rel_tol=1e-9)    # outside
    assert values_match(float("nan"), float("nan"))
    assert values_match(0.0, 1e-13)  # abs_tol backstop near zero


def test_table_diff_passed_logic():
    ok = TableDiff("t", 10, 10, 0, 0, [ColumnDiff("x", 0, 10, [])])
    bad = TableDiff("t", 10, 10, 0, 0, [ColumnDiff("x", 3, 10, ["r1: 1 != 2"])])
    counts = TableDiff("t", 10, 9, 1, 0, [])
    assert ok.passed and not bad.passed and not counts.passed


def test_diff_report_to_text_mentions_columns_and_truncates():
    d = DiffReport("p1", [TableDiff("t", 10, 10, 0, 0,
                                    [ColumnDiff("balance", 4, 10, ["id=3: 1.0 != 2.0"])])])
    text = d.to_text()
    assert "balance" in text and "4/10" in text and "id=3" in text
    assert not d.passed
    assert len(d.to_text(max_chars=100)) <= 120  # truncation honored (+ marker)
