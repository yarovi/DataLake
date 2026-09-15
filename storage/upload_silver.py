from pathlib import Path

from storage.blob_client_general import (
    create_container,
    upload_to_container
)


SILVER_CONTAINER = "silver"
SILVER_LOCAL_PATH = Path("staging/silver/sales")


def main():

    create_container(SILVER_CONTAINER)

    parquet_files = list(
        SILVER_LOCAL_PATH.glob("part-*.parquet")
    )

    print(
        f"[SILVER-UPLOAD] Archivos encontrados: "
        f"{len(parquet_files)}"
    )

    for parquet_file in parquet_files:

        blob_name = f"sales/{parquet_file.name}"

        upload_to_container(
            container_name=SILVER_CONTAINER,
            local_file_path=str(parquet_file),
            blob_name=blob_name
        )

    print("[SILVER-UPLOAD] Finalizado")


if __name__ == "__main__":
    main()