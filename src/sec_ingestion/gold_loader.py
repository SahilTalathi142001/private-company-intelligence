from pathlib import Path

import pandas as pd


def load_single_parquet(
    directory: Path,
) -> pd.DataFrame:

    files = list(
        directory.glob("*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            f"No parquet file found in: {directory}"
        )

    if len(files) > 1:
        raise ValueError(
            f"Expected one parquet file in "
            f"{directory}, found {len(files)}"
        )

    return pd.read_parquet(files[0])