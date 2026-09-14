from pathlib import Path

import pandas as pd


def load_sales(
    dataframe: pd.DataFrame,
    output_path: str = "output/sales.parquet"
) -> None:

    path = Path(output_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dataframe.to_parquet(
        path,
        engine="pyarrow",
        index=False
    )

    print(f"[LOAD] Archivo generado: {path}")