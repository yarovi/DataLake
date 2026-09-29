from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os



GOLD_PATH = "/opt/spark/staging/gold"


def fail_if(condition: bool, message: str) -> None:
    if condition:
        raise ValueError(f"[DQ] ERROR - {message}")


def validate_country(spark: SparkSession) -> None:
    print("\n=== DQ: sales_by_country ===")

    #df = spark.read.parquet(f"{GOLD_PATH}/sales_by_country")
    df = spark.read.parquet(f"{GOLD_PATH}/sales_by_country")
    rows = df.count()
    print(f"[DQ] Registros: {rows}")

    fail_if(rows == 0, "sales_by_country está vacío")

    nulls = df.filter(
        F.col("customer_country").isNull()
        | (F.trim(F.col("customer_country")) == "")
        | F.col("total_sales").isNull()
    ).count()

    print(f"[DQ] Nulos/campos vacíos: {nulls}")
    fail_if(nulls > 0, "sales_by_country contiene valores obligatorios nulos")

    duplicates = (
        df.groupBy("customer_country")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"[DQ] Países duplicados: {duplicates}")
    fail_if(duplicates > 0, "customer_country contiene duplicados")

    negatives = df.filter(F.col("total_sales") < 0).count()

    print(f"[DQ] Ventas negativas: {negatives}")
    fail_if(negatives > 0, "sales_by_country contiene ventas negativas")

    print("[DQ] OK - sales_by_country")


def validate_month(spark: SparkSession) -> None:
    print("\n=== DQ: sales_by_month ===")

    df = spark.read.parquet(f"{GOLD_PATH}/sales_by_month")

    rows = df.count()
    print(f"[DQ] Registros: {rows}")

    fail_if(rows == 0, "sales_by_month está vacío")

    nulls = df.filter(
        F.col("year").isNull()
        | F.col("month").isNull()
        | F.col("total_sales").isNull()
    ).count()

    print(f"[DQ] Nulos: {nulls}")
    fail_if(nulls > 0, "sales_by_month contiene valores obligatorios nulos")

    duplicates = (
        df.groupBy("year", "month")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"[DQ] Períodos duplicados: {duplicates}")
    fail_if(duplicates > 0, "existen períodos year/month duplicados")

    invalid_months = df.filter(
        (F.col("month") < 1) | (F.col("month") > 12)
    ).count()

    print(f"[DQ] Meses fuera de rango: {invalid_months}")
    fail_if(invalid_months > 0, "existen meses fuera del rango 1..12")

    negatives = df.filter(F.col("total_sales") < 0).count()

    print(f"[DQ] Ventas negativas: {negatives}")
    fail_if(negatives > 0, "sales_by_month contiene ventas negativas")

    print("[DQ] OK - sales_by_month")


def validate_product(spark: SparkSession) -> None:
    print("\n=== DQ: sales_by_product ===")

    df = spark.read.parquet(
        f"{GOLD_PATH}/sales_by_product"
    )

    # Solo para probar que el Quality Gate detecta datos incorrectos.
    # NO modifica físicamente el Parquet.
    if os.getenv("DQ_INJECT_ERROR") == "true":
        print(
            "[DQ-TEST] Inyectando units_sold = -10 "
            "en product_id = 38"
        )

        df = df.withColumn(
            "units_sold",
            F.when(
                F.col("product_id") == 38,
                F.lit(-10)
            ).otherwise(F.col("units_sold"))
        )

    rows = df.count()
    print(f"[DQ] Registros: {rows}")

    fail_if(
        rows == 0,
        "sales_by_product está vacío"
    )

    nulls = df.filter(
        F.col("product_id").isNull()
        | F.col("product_name").isNull()
        | (F.trim(F.col("product_name")) == "")
        | F.col("units_sold").isNull()
        | F.col("total_sales").isNull()
    ).count()

    print(f"[DQ] Nulos/campos vacíos: {nulls}")

    fail_if(
        nulls > 0,
        "sales_by_product contiene valores obligatorios nulos"
    )

    duplicates = (
        df.groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"[DQ] product_id duplicados: {duplicates}")

    fail_if(
        duplicates > 0,
        "product_id contiene duplicados"
    )

    invalid_values = df.filter(
        (F.col("units_sold") < 0)
        | (F.col("total_sales") < 0)
    ).count()

    print(f"[DQ] Valores negativos: {invalid_values}")

    fail_if(
        invalid_values > 0,
        "existen unidades o ventas negativas"
    )

    print("[DQ] OK - sales_by_product")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NorthwindGoldDataQuality")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=== DATA QUALITY - GOLD CONTENT ===")

        validate_country(spark)
        validate_month(spark)
        validate_product(spark)

        print("\n[DQ] TODAS LAS VALIDACIONES DE CONTENIDO PASARON")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()