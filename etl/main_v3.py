from etl.extract_raw import extract_table
from etl.load_bronze_v2 import load_to_bronze


TABLES = [
    "orders",
    "customers",
    "products",
    "order_details"
]


def main():

    for table_name in TABLES:

        print(f"\n[ELT] Procesando tabla: {table_name}")

        dataframe = extract_table(table_name)

        print(
            f"[ELT-EXTRACT] {table_name}: "
            f"{len(dataframe)} registros"
        )

        load_to_bronze(
            dataframe=dataframe,
            table_name=table_name
        )

    print("\n[ELT] Carga Bronze finalizada")


if __name__ == "__main__":
    main()