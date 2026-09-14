from extract_v2 import extract_sales
from transform import transform_sales
from load import load_sales


def main():

    # E - Extract
    sales = extract_sales()

    print(
        f"[EXTRACT] Registros extraídos: "
        f"{len(sales)}"
    )

    # T - Transform
    transformed_sales = transform_sales(sales)

    print(
        f"[TRANSFORM] Registros transformados: "
        f"{len(transformed_sales)}"
    )

    # L - Load
    load_sales(transformed_sales)


if __name__ == "__main__":
    main()