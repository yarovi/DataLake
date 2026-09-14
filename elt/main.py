from extract_raw import extract_table


def main():
    orders = extract_table("orders")

    print(f"[ELT-EXTRACT] Registros orders: {len(orders)}")

    print(orders.head())


if __name__ == "__main__":
    main()