from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_INPUT_PATH = "/opt/spark/staging/silver_input/sales"

GOLD_COUNTRY_OUTPUT_PATH = (
    "/opt/spark/staging/gold/sales_by_country"
)

GOLD_MONTH_OUTPUT_PATH = (
    "/opt/spark/staging/gold/sales_by_month"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("northwind-silver-to-gold")
        .getOrCreate()
    )


def main():

    # =========================================================
    # 1. SPARK
    # =========================================================

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")


    # =========================================================
    # 2. LEER SILVER
    # =========================================================

    print("\n=== LECTURA SILVER ===")
    print(f"[SILVER] Leyendo: {SILVER_INPUT_PATH}")

    silver_df = spark.read.parquet(
        SILVER_INPUT_PATH
    )

    print("\n=== SILVER SCHEMA ===")
    silver_df.printSchema()

    silver_count = silver_df.count()

    print(
        f"\n[SILVER] registros recuperados: "
        f"{silver_count}"
    )

    print("\n=== MUESTRA SILVER ===")

    silver_df.show(
        5,
        truncate=False
    )


    # =========================================================
    # 3. TOTAL BASE SILVER
    # =========================================================

    print("\n=== TOTAL BASE SILVER ===")

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

    print(
        f"[SILVER] Total ventas: "
        f"{silver_total}"
    )


    # =========================================================
    # 4. GOLD - SALES BY COUNTRY
    # =========================================================

    print("\n=== GOLD: SALES BY COUNTRY ===")

    sales_by_country_df = (
        silver_df
        .groupBy(
            "customer_country"
        )
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

    country_count = sales_by_country_df.count()

    sales_by_country_df.show(
        country_count,
        truncate=False
    )

    print(
        f"[GOLD] países generados: "
        f"{country_count}"
    )


    # =========================================================
    # 5. RECONCILIAR GOLD COUNTRY
    # =========================================================

    print(
        "\n=== RECONCILIACION SALES BY COUNTRY ==="
    )

    country_total = (
        sales_by_country_df
        .agg(
            F.round(
                F.sum("total_sales"),
                2
            ).alias("total")
        )
        .first()["total"]
    )

    print(
        f"[RECON-COUNTRY] Total Silver: "
        f"{silver_total}"
    )

    print(
        f"[RECON-COUNTRY] Total Gold:   "
        f"{country_total}"
    )

    if silver_total == country_total:
        print(
            "[RECON-COUNTRY] OK - "
            "Silver y Gold por país coinciden"
        )
    else:
        print(
            "[RECON-COUNTRY] ERROR - "
            "Los totales no coinciden"
        )


    # =========================================================
    # 6. GOLD - SALES BY MONTH
    # =========================================================

    print("\n=== GOLD: SALES BY MONTH ===")

    sales_by_month_df = (
        silver_df
        .withColumn(
            "year",
            F.year("order_date")
        )
        .withColumn(
            "month",
            F.month("order_date")
        )
        .groupBy(
            "year",
            "month"
        )
        .agg(
            F.round(
                F.sum("line_total"),
                2
            ).alias("total_sales")
        )
        .orderBy(
            "year",
            "month"
        )
    )

    month_count = sales_by_month_df.count()

    sales_by_month_df.show(
        month_count,
        truncate=False
    )

    print(
        f"[GOLD] meses generados: "
        f"{month_count}"
    )


    # =========================================================
    # 7. RECONCILIAR GOLD MONTH
    # =========================================================

    print(
        "\n=== RECONCILIACION SALES BY MONTH ==="
    )

    monthly_total = (
        sales_by_month_df
        .agg(
            F.round(
                F.sum("total_sales"),
                2
            ).alias("total")
        )
        .first()["total"]
    )

    print(
        f"[RECON-MONTH] Total Silver: "
        f"{silver_total}"
    )

    print(
        f"[RECON-MONTH] Total Gold:   "
        f"{monthly_total}"
    )

    if silver_total == monthly_total:
        print(
            "[RECON-MONTH] OK - "
            "Silver y Gold mensual coinciden"
        )
    else:
        print(
            "[RECON-MONTH] ERROR - "
            "Los totales no coinciden"
        )


    # =========================================================
    # 8. ESCRIBIR GOLD COUNTRY
    # =========================================================

    print(
        f"\n[GOLD] Escribiendo: "
        f"{GOLD_COUNTRY_OUTPUT_PATH}"
    )

    (
        sales_by_country_df
        .write
        .mode("overwrite")
        .parquet(
            GOLD_COUNTRY_OUTPUT_PATH
        )
    )

    print(
        "[GOLD] sales_by_country "
        "escrito correctamente"
    )


    # =========================================================
    # 9. ESCRIBIR GOLD MONTH
    # =========================================================

    print(
        f"\n[GOLD] Escribiendo: "
        f"{GOLD_MONTH_OUTPUT_PATH}"
    )

    (
        sales_by_month_df
        .write
        .mode("overwrite")
        .parquet(
            GOLD_MONTH_OUTPUT_PATH
        )
    )

    print(
        "[GOLD] sales_by_month "
        "escrito correctamente"
    )


    # =========================================================
    # 10. FINALIZAR
    # =========================================================

    print("\n=== GOLD FINALIZADO ===")

    spark.stop()


if __name__ == "__main__":
    main()