from pyspark.sql import SparkSession


SILVER_INPUT_PATH = "/opt/spark/staging/silver_input/sales"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("northwind-silver-to-gold")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== LECTURA SILVER ===")
    print(f"[SILVER] Leyendo: {SILVER_INPUT_PATH}")

    silver_df = spark.read.parquet(SILVER_INPUT_PATH)

    print("\n=== SILVER SCHEMA ===")
    silver_df.printSchema()

    silver_count = silver_df.count()

    print(f"\n[SILVER] registros recuperados: {silver_count}")

    print("\n=== MUESTRA SILVER ===")
    silver_df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()