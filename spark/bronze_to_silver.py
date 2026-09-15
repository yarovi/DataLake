from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STAGING_BASE_PATH = "/opt/spark/staging"

TABLES = [
    "orders",
    "customers",
    "products",
    "order_details"
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("northwind-bronze-to-silver")
        .getOrCreate()
    )


def read_bronze_table(spark, table_name):
    path = f"{STAGING_BASE_PATH}/{table_name}/{table_name}.parquet"

    print(f"\n[BRONZE] Leyendo: {path}")

    return spark.read.parquet(path)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    dataframes = {}

    # 1. Leer los 4 Bronze
    for table_name in TABLES:
        dataframe = read_bronze_table(spark, table_name)
        dataframes[table_name] = dataframe

        print(f"\n=== {table_name.upper()} ===")
        dataframe.printSchema()

        total = dataframe.count()
        print(f"[COUNT] {table_name}: {total}")

    # 2. Obtener cada DataFrame
    orders_df = dataframes["orders"]
    customers_df = dataframes["customers"]
    products_df = dataframes["products"]
    order_details_df = dataframes["order_details"]

    # 3. Validar claves
    print("\n=== VALIDACION DE CLAVES ===")

    print(
        "[PK] orders:",
        orders_df.count(),
        "/ unicos:",
        orders_df.select("order_id").distinct().count()
    )

    print(
        "[PK] customers:",
        customers_df.count(),
        "/ unicos:",
        customers_df.select("customer_id").distinct().count()
    )

    print(
        "[PK] products:",
        products_df.count(),
        "/ unicos:",
        products_df.select("product_id").distinct().count()
    )

    print(
        "[PK COMPUESTA] order_details:",
        order_details_df.count(),
        "/ unicos:",
        order_details_df
            .select("order_id", "product_id")
            .distinct()
            .count()
    )

    #NEW CODE
    print("\n=== VALIDACION DE FOREIGN KEYS ===")

    orders_orphans = (
        order_details_df
        .join(
            orders_df.select("order_id"),
            on="order_id",
            how="left_anti"
        )
    )

    products_orphans = (
        order_details_df
        .join(
            products_df.select("product_id"),
            on="product_id",
            how="left_anti"
        )
    )

    customers_orphans = (
        orders_df
        .join(
            customers_df.select("customer_id"),
            on="customer_id",
            how="left_anti"
        )
    )

    print(
        "[FK] order_details -> orders huérfanos:",
        orders_orphans.count()
    )

    print(
        "[FK] order_details -> products huérfanos:",
        products_orphans.count()
    )

    print(
        "[FK] orders -> customers huérfanos:",
        customers_orphans.count()
    )
    #END NEW CODE

    print("\n=== CONSTRUCCION SILVER SALES ===")

    silver_sales_df = (
        order_details_df.alias("od")

        .join(
            orders_df.alias("o"),
            F.col("od.order_id") == F.col("o.order_id"),
            "left"
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
            F.col("od.order_id").alias("order_id"),
            F.col("od.product_id").alias("product_id"),

            F.col("o.customer_id").alias("customer_id"),
            F.col("o.employee_id").alias("employee_id"),
            F.col("o.order_date").alias("order_date"),

            F.col("c.company_name").alias("customer_name"),
            F.col("c.country").alias("customer_country"),

            F.col("p.product_name").alias("product_name"),
            F.col("p.category_id").alias("category_id"),
            F.col("p.supplier_id").alias("supplier_id"),

            F.col("od.quantity").alias("quantity"),
            F.col("od.unit_price").alias("unit_price"),
            F.col("od.discount").alias("discount"),

            F.round(
                F.col("od.quantity")
                * F.col("od.unit_price")
                * (F.lit(1) - F.col("od.discount")),
                2
            ).alias("line_total")
        )
    )

    print("\n=== SILVER SALES SCHEMA ===")
    silver_sales_df.printSchema()

    silver_count = silver_sales_df.count()

    print(f"\n[SILVER] registros: {silver_count}")

    print("\n=== MUESTRA SILVER SALES ===")
    silver_sales_df.show(10, truncate=False)
    #END NEW CODE 

    #NEW CODE
    SILVER_OUTPUT_PATH = "/opt/spark/staging/silver/sales"

    print(f"\n[SILVER] Escribiendo dataset en: {SILVER_OUTPUT_PATH}")

    (
        silver_sales_df
        .write
        .mode("overwrite")
        .parquet(SILVER_OUTPUT_PATH)
    )

    print("[SILVER] Escritura completada")
    #END NEW CODE

    spark.stop()


if __name__ == "__main__":
    main()