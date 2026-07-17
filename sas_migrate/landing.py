"""Landing utilities: SAS-quirk normalization applied to BOTH sides of every
comparison (spec section 3), source readers, and snapshot content hashing.
Zero-dependency path is CSV/Parquet exported from the SAS side; pyreadstat is
optional (JFrog-governed) for direct .sas7bdat reads."""
from __future__ import annotations

import hashlib
import math
from datetime import date, timedelta

import pandas as pd

SAS_EPOCH = date(1960, 1, 1)


class MissingDependencyError(Exception):
    pass


def sas_date_to_iso(v) -> str | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return (SAS_EPOCH + timedelta(days=int(f))).isoformat()


def normalize_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        s = v.rstrip()
        if s in ("", "."):
            return None
        return s
    return v


def normalize_frame(df: pd.DataFrame, date_cols: list[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    for col in date_cols or []:
        out[col] = out[col].map(sas_date_to_iso)
    for col in out.columns:
        if col in (date_cols or []):
            continue
        out[col] = out[col].map(normalize_value)
    return out.astype(object).where(pd.notnull(out), None)


def content_hash(df: pd.DataFrame) -> str:
    """Order-insensitive content hash for snapshot naming (point-in-time id)."""
    lines = sorted(
        "\x1f".join("" if v is None or (isinstance(v, float) and math.isnan(v))
                    else str(v) for v in row)
        for row in df.itertuples(index=False, name=None))
    digest = hashlib.sha256("\n".join([",".join(df.columns), *lines]).encode())
    return digest.hexdigest()[:12]


def read_source(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".parquet"):
        return pd.read_parquet(path)
    if lower.endswith(".sas7bdat"):
        try:
            import pyreadstat  # noqa: PLC0415 - optional, JFrog-governed
        except ImportError as e:
            raise MissingDependencyError(
                "Reading .sas7bdat requires pyreadstat, which needs JFrog approval. "
                "Zero-dependency fallback: export the dataset from SAS as CSV/Parquet "
                "(PROC EXPORT) and land that instead.") from e
        df, _meta = pyreadstat.read_sas7bdat(path)
        return df
    raise ValueError(f"unsupported source format: {path}")
