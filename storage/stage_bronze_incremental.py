from storage.download_blob_v4 import download_blob


def stage_incremental_batch(
    start_order_id: int,
    end_order_id: int
):
    batch_id = f"{start_order_id}_{end_order_id}"

    tables = [
        "orders",
        "order_details"
    ]

    print(
        f"[STAGING-INCREMENTAL] Preparando batch: {batch_id}"
    )

    for table in tables:
        blob_name = (
            f"{table}/incremental/"
            f"{table}_{batch_id}.parquet"
        )

        destination = (
            f"staging/incremental/"
            f"{batch_id}/{table}.parquet"
        )

        print(
            f"[STAGING-INCREMENTAL] "
            f"{blob_name} -> {destination}"
        )

        download_blob(
            container_name="bronze",
            blob_name=blob_name,
            destination=destination
        )

    print(
        f"[STAGING-INCREMENTAL] Batch {batch_id} preparado"
    )