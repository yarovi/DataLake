from storage.download_blob_v4 import download_blob


BRONZE_TABLES = [
    "orders",
    "customers",
    "products",
    "order_details"
]


def main():
    for table in BRONZE_TABLES:
        download_blob(
            container_name="bronze",
            blob_name=f"{table}/{table}.parquet",
            destination=f"staging/{table}/{table}.parquet"
        )


if __name__ == "__main__":
    main()