from storage.download_blob import download_blob


BRONZE_TABLES = [
    "orders",
    "customers",
    "products",
    "order_details"
]


def main():
    for table in BRONZE_TABLES:
        download_blob(
            blob_name=f"{table}/{table}.parquet",
            destination=f"staging/{table}/{table}.parquet"
        )


if __name__ == "__main__":
    main()