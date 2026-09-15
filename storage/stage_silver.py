from pathlib import Path

from storage.blob_client_general import get_blob_service_client
from storage.download_blob_v4 import download_blob


SILVER_CONTAINER = "silver"
SILVER_PREFIX = "sales/"
SILVER_STAGING_PATH = Path("staging/silver_input/sales")


def main():
    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        SILVER_CONTAINER
    )

    blobs = container_client.list_blobs(
        name_starts_with=SILVER_PREFIX
    )

    parquet_blobs = [
        blob.name
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]

    print(
        f"[SILVER-STAGING] Parquet encontrados: "
        f"{len(parquet_blobs)}"
    )

    for blob_name in parquet_blobs:

        file_name = Path(blob_name).name

        destination = (
            SILVER_STAGING_PATH / file_name
        )

        download_blob(
            container_name=SILVER_CONTAINER,
            blob_name=blob_name,
            destination=str(destination)
        )

    print("[SILVER-STAGING] Finalizado")


if __name__ == "__main__":
    main()