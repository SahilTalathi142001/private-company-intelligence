import json
from pathlib import Path

import pandas as pd


def build_metric_dimension(
    config_path: Path,
) -> pd.DataFrame:

    if not config_path.exists():
        raise FileNotFoundError(
            f"Metric catalog not found: {config_path}"
        )

    with open(
        config_path,
        "r",
        encoding="utf-8",
    ) as f:
        config = json.load(f)

    records = []

    for metric_id, metadata in config.items():

        record = {
            "metric_id": metric_id,
            "metric_name": metadata["metric_name"],
            "category": metadata["category"],
            "statement": metadata["statement"],
            "data_type": metadata["data_type"],
            "unit_type": metadata["unit_type"],
        }

        records.append(record)

    df = pd.DataFrame(records)

    # Basic validation
    required_columns = [
        "metric_id",
        "metric_name",
        "category",
        "statement",
        "data_type",
        "unit_type",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing metric columns: "
            + ", ".join(missing_columns)
        )

    if df["metric_id"].duplicated().any():
        duplicates = (
            df.loc[
                df["metric_id"].duplicated(),
                "metric_id",
            ]
            .tolist()
        )

        raise ValueError(
            f"Duplicate metric IDs: {duplicates}"
        )

    if df["data_type"].isin(
        ["flow", "stock"]
    ).all() is False:
        raise ValueError(
            "Invalid data_type. "
            "Expected only 'flow' or 'stock'."
        )

    return df.sort_values(
        "metric_id"
    ).reset_index(drop=True)