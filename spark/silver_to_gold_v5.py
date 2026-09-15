from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

    #NEW CODE
    print("\n=== GOLD: SALES BY COUNTRY ===")

    sales_by_country_df = (
        silver_df
        .groupBy("customer_country")
        .agg(
            F.round(
                F.sum("line_total"),
                2
            ).alias("total_sales")
        )
        .orderBy(
            F.col("total_sales").desc()
        )
    )

    sales_by_country_df.show(
        truncate=False
    )

    country_count = sales_by_country_df.count()

    print(
        f"[GOLD] países generados: {country_count}"
    )
    #END NEW CODE

    #NEW CODE
    print("\n=== RECONCILIACION SILVER VS GOLD ===")

    silver_total = (
        silver_df
        .agg(
            F.round(
                F.sum("line_total"),
                2
            ).alias("total")
        )
        .first()["total"]
    )

    gold_total = (
        sales_by_country_df
        .agg(
            F.round(
                F.sum("total_sales"),
                2
            ).alias("total")
        )
        .first()["total"]
    )

    print(f"[RECON] Total Silver: {silver_total}")
    print(f"[RECON] Total Gold:   {gold_total}")

    if silver_total == gold_total:
        print("[RECON] OK - Silver y Gold coinciden")
    else:
        print("[RECON] ERROR - Los totales no coinciden")
    #END NEW CODE

    #NEW CODE
    GOLD_OUTPUT_PATH = "/opt/spark/staging/gold/sales_by_country"

    print(
        f"\n[GOLD] Escribiendo dataset en: "
        f"{GOLD_OUTPUT_PATH}"
    )

    (
        sales_by_country_df
        .write
        .mode("overwrite")
        .parquet(GOLD_OUTPUT_PATH)
    )

    print("[GOLD] Escritura completada")
    #END NEW CODE

    spark.stop()


if __name__ == "__main__":
    main()