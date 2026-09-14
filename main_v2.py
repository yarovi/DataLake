from extract_v2 import extract_sales
from transform import transform_sales


def main():

    sales = extract_sales()

    print(f"[EXTRACT] Registros extraídos: {len(sales)}")

    transformed_sales = transform_sales(sales)

    print(
        f"[TRANSFORM] Registros transformados: "
        f"{len(transformed_sales)}"
    )

    print()

    print(
        transformed_sales[
            [
                "order_id",
                "product_name",
                "quantity",
                "unit_price",
                "discount",
                "total",
            ]
        ].head(10)
    )


if __name__ == "__main__":
    main()