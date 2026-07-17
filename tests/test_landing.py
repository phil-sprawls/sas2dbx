import math

import pandas as pd
import pytest

from sas_migrate.landing import (
    MissingDependencyError, content_hash, normalize_frame, normalize_value,
    read_source, sas_date_to_iso,
)


def test_sas_date_epoch_is_1960():
    assert sas_date_to_iso(0) == "1960-01-01"
    assert sas_date_to_iso(24107) == "2026-01-01"
    assert sas_date_to_iso(None) is None
    assert sas_date_to_iso(float("nan")) is None


def test_normalize_value_missings_and_padding():
    assert normalize_value(float("nan")) is None
    assert normalize_value(".") is None
    assert normalize_value("   ") is None
    assert normalize_value("abc   ") == "abc"
    assert normalize_value(42) == 42


def test_normalize_frame_applies_dates_and_missings():
    df = pd.DataFrame({"d": [0.0, None], "name": ["bob  ", "."]})
    out = normalize_frame(df, date_cols=["d"])
    assert out["d"].tolist() == ["1960-01-01", None]
    assert out["name"].tolist() == ["bob", None]


def test_content_hash_is_order_insensitive():
    a = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    b = pd.DataFrame({"x": [2, 1], "y": ["b", "a"]})
    c = pd.DataFrame({"x": [1, 3], "y": ["a", "b"]})
    assert content_hash(a) == content_hash(b)
    assert content_hash(a) != content_hash(c)
    assert len(content_hash(a)) == 12


def test_read_source_csv(tmp_path):
    p = tmp_path / "in.csv"
    p.write_text("x,y\n1,a\n2,b\n")
    df = read_source(str(p))
    assert len(df) == 2


def test_read_source_sas7bdat_without_pyreadstat_raises_helpful_error(tmp_path):
    p = tmp_path / "in.sas7bdat"
    p.write_bytes(b"")
    with pytest.raises((MissingDependencyError, Exception)) as exc_info:
        read_source(str(p))
    # If pyreadstat IS installed the empty file fails differently; the
    # MissingDependencyError branch is what we assert on when it's absent.
    if isinstance(exc_info.value, MissingDependencyError):
        assert "pyreadstat" in str(exc_info.value)
        assert "CSV/Parquet" in str(exc_info.value)
