"""A promoted row needs an accession or an as-of date. Both empty means it stays out."""

from __future__ import annotations

import pandas as pd


def is_present(value) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text.lower() not in {"", "nan", "none", "nat"}


def _any_present(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    mask = pd.Series(False, index=frame.index)
    for column in columns:
        if column not in frame.columns:
            continue
        mask = mask | frame[column].map(is_present)
    return mask


def retain_lineaged_rows(
    frame: pd.DataFrame,
    accession_columns: tuple[str, ...] = ("accession_number", "source_accession_number"),
    as_of_columns: tuple[str, ...] = ("as_of_date", "filing_date", "period_end"),
) -> pd.DataFrame:
    """Keep rows that state an accession or an as-of date. Do not fill either field."""
    if frame.empty:
        return frame
    keep = _any_present(frame, accession_columns) | _any_present(frame, as_of_columns)
    return frame.loc[keep].copy()
