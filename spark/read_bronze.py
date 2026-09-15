from pyspark.sql import SparkSession


BRONZE_ORDERS_PATH = "/opt/spark/staging/orders/orders.parquet"


def main():
    spark = (
        SparkSession.builder
        .appName("northwind-read-bronze")
        .getOrCreate()
    )

    print("\n=== LEYENDO BRONZE ORDERS ===")

    orders_df = spark.read.parquet(BRONZE_ORDERS_PATH)

    print("\n=== SCHEMA ===")
    orders_df.printSchema()

    print("\n=== PRIMEROS 5 REGISTROS ===")
    orders_df.show(5, truncate=False)

    print("\n=== TOTAL REGISTROS ===")
    print(f"Orders: {orders_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()