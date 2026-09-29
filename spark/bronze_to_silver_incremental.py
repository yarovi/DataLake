from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys


STAGING_BASE_PATH = "/opt/spark/staging"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("northwind-bronze-to-silver-incremental")
        .getOrCreate()
    )


def main():
    if len(sys.argv) != 3:
        raise ValueError(
            "Uso: bronze_to_silver_incremental.py "
            "<start_order_id> <end_order_id>"
        )

    start_order_id = int(sys.argv[1])
    end_order_id = int(sys.argv[2])

    batch_id = f"{start_order_id}_{end_order_id}"

    print(f"[INCREMENTAL-SPARK] Batch: {batch_id}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # --------------------------------------------------
        # 1. Datos incrementales
        # --------------------------------------------------

        incremental_path = (
            f"{STAGING_BASE_PATH}/incremental/{batch_id}"
        )

        orders_df = spark.read.parquet(
            f"{incremental_path}/orders.parquet"
        )

        order_details_df = spark.read.parquet(
            f"{incremental_path}/order_details.parquet"
        )

        # --------------------------------------------------
        # 2. Dimensiones de referencia
        # --------------------------------------------------

        customers_df = spark.read.parquet(
            f"{STAGING_BASE_PATH}/customers/customers.parquet"
        )

        products_df = spark.read.parquet(
            f"{STAGING_BASE_PATH}/products/products.parquet"
        )

        print(
            "[INCREMENTAL-SPARK] Orders:",
            orders_df.count()
        )

        print(
            "[INCREMENTAL-SPARK] Order details:",
            order_details_df.count()
        )

        # --------------------------------------------------
        # 3. Seguridad: validar rango
        # --------------------------------------------------

        invalid_orders = orders_df.filter(
            (F.col("order_id") < start_order_id)
            | (F.col("order_id") > end_order_id)
        ).count()

        invalid_details = order_details_df.filter(
            (F.col("order_id") < start_order_id)
            | (F.col("order_id") > end_order_id)
        ).count()

        if invalid_orders > 0 or invalid_details > 0:
            raise ValueError(
                "[INCREMENTAL-SPARK] "
                "El batch contiene registros fuera del rango"
            )

        # --------------------------------------------------
        # 4. Construir Silver incremental
        # --------------------------------------------------

        silver_df = (
            orders_df.alias("o")
            .join(
                order_details_df.alias("od"),
                F.col("o.order_id") == F.col("od.order_id"),
                "inner"
            )
            .join(
                customers_df.alias("c"),
                F.col("o.customer_id") == F.col("c.customer_id"),
                "left"
            )
            .join(
                products_df.alias("p"),
                F.col("od.product_id") == F.col("p.product_id"),
                "left"
            )
            .select(
                F.col("o.order_id").alias("order_id"),
                F.col("o.order_date").alias("order_date"),
                F.col("o.customer_id").alias("customer_id"),
                F.col("c.company_name").alias("customer_name"),
                F.col("c.country").alias("customer_country"),
                F.col("od.product_id").alias("product_id"),
                F.col("p.product_name").alias("product_name"),
                F.col("od.quantity").alias("quantity"),
                F.col("od.unit_price").alias("unit_price"),
                F.col("od.discount").alias("discount"),
                (
                    F.col("od.quantity")
                    * F.col("od.unit_price")
                    * (F.lit(1) - F.col("od.discount"))
                ).alias("line_total")
            )
        )

        silver_count = silver_df.count()

        print(
            f"[INCREMENTAL-SPARK] Silver generado: "
            f"{silver_count} registros"
        )

        # --------------------------------------------------
        # 5. Validaciones mínimas
        # --------------------------------------------------

        null_products = silver_df.filter(
            F.col("product_name").isNull()
        ).count()

        null_customers = silver_df.filter(
            F.col("customer_name").isNull()
        ).count()

        if null_products > 0:
            raise ValueError(
                f"[INCREMENTAL-SPARK] "
                f"Productos no encontrados: {null_products}"
            )

        if null_customers > 0:
            raise ValueError(
                f"[INCREMENTAL-SPARK] "
                f"Clientes no encontrados: {null_customers}"
            )

        # --------------------------------------------------
        # 6. Escribir candidato Silver
        # --------------------------------------------------

        output_path = (
            f"{STAGING_BASE_PATH}/silver_incremental/{batch_id}"
        )

        (
            silver_df
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(
            f"[INCREMENTAL-SPARK] Silver escrito en: "
            f"{output_path}"
        )

        print(
            "[INCREMENTAL-SPARK] "
            "Watermark NO modificado"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()