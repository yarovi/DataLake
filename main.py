from extract import extract_sales


def main():
    sales = extract_sales()

    print(f"Registros extraídos: {len(sales)}")
    print()
    print(sales.head(10))


if __name__ == "__main__":
    main()