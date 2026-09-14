from elt.extract_raw import extract_table
from elt.load_bronze_v2 import load_to_bronze


def main():
    orders = extract_table("orders")

    print(
        f"[ELT-EXTRACT] Registros orders: {len(orders)}"
    )

    load_to_bronze(
        dataframe=orders,
        table_name="orders"
    )


if __name__ == "__main__":
    main()