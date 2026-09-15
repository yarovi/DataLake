from pathlib import Path

from storage.blob_client_general import (
    create_container,
    upload_to_container
)


GOLD_CONTAINER = "gold"

GOLD_DATASETS = [
    "sales_by_country"
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