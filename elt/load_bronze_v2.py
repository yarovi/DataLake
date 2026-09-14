from pathlib import Path

import pandas as pd

from storage.blob_client_v2 import upload_file


def load_to_bronze(
    dataframe: pd.DataFrame,
    table_name: str
):
    local_path = Path(
        f"output/bronze/{table_name}.parquet"
    )

    local_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dataframe.to_parquet(
        local_path,
        engine="pyarrow",
        index=False
    )

    blob_name = (
        f"{table_name}/{table_name}.parquet"
    )

    upload_file(
        local_file_path=str(local_path),
        blob_name=blob_name
    )

    print(
        f"[ELT-LOAD] {table_name} cargado a bronze"
    )