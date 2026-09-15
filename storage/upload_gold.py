from pathlib import Path
from storage.blob_client_general import delete_blobs_by_prefix

from storage.blob_client_general import (
    create_container,
    upload_to_container
)


GOLD_CONTAINER = "gold"

GOLD_DATASETS = [
    "sales_by_country",
    "sales_by_month",
    "sales_by_product"
]


def main():

    create_container(GOLD_CONTAINER)

    for dataset in GOLD_DATASETS:

        local_path = Path(
            f"staging/gold/{dataset}"
        )

        parquet_files = list(
            local_path.glob("part-*.parquet")
        )

        print(
            f"[GOLD-UPLOAD] {dataset}: "
            f"{len(parquet_files)} archivos"
        )

        # IMPORTANTE:
        # Solo borramos el dataset remoto si
        # tenemos archivos locales para reemplazarlo.
        if not parquet_files:
            print(
                f"[GOLD-UPLOAD] ERROR - "
                f"No existen archivos para {dataset}"
            )
            continue

        # Limpiar versión anterior en Azurite
        delete_blobs_by_prefix(
            container_name=GOLD_CONTAINER,
            prefix=f"{dataset}/"
        )

        # Subir versión actual
        for parquet_file in parquet_files:

            blob_name = (
                f"{dataset}/{parquet_file.name}"
            )

            upload_to_container(
                container_name=GOLD_CONTAINER,
                local_file_path=str(parquet_file),
                blob_name=blob_name
            )

    print("[GOLD-UPLOAD] Finalizado")


if __name__ == "__main__":
    main()