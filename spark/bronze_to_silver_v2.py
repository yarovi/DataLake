from pyspark.sql import SparkSession


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

    for table_name in TABLES:
        dataframe = read_bronze_table(spark, table_name)

        dataframes[table_name] = dataframe

        print(f"\n=== {table_name.upper()} ===")
        dataframe.printSchema()

        total = dataframe.count()

        print(f"[COUNT] {table_name}: {total}")

    spark.stop()


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

    spark.stop()