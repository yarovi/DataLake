from pathlib import Path

import pandas as pd

from storage.blob_client_v2 import upload_file


def load_incremental_to_bronze(
    dataframe: pd.DataFrame,
    table_name: str,
    start_order_id: int,
    end_order_id: int
):
    if dataframe.empty:
        raise ValueError(
            "[ELT-LOAD] No se puede crear un batch vacío"
        )

    batch_name = (
        f"{table_name}_{start_order_id}_{end_order_id}.parquet"
    )

    local_path = Path(
        f"output/bronze/{table_name}/{batch_name}"
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
        f"{table_name}/incremental/{batch_name}"
    )

    upload_file(
        local_file_path=str(local_path),
        blob_name=blob_name
    )

    print(
        f"[ELT-LOAD] Batch incremental cargado: "
        f"{blob_name}"
    )

    return blob_name