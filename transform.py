import pandas as pd


REQUIRED_COLUMNS = {
    "order_id",
    "order_date",
    "customer",
    "employee",
    "product_name",
    "quantity",
    "unit_price",
    "discount",
}


def transform_sales(dataframe: pd.DataFrame) -> pd.DataFrame:

    missing_columns = REQUIRED_COLUMNS - set(dataframe.columns)

    if missing_columns:
        raise ValueError(
            f"Faltan columnas requeridas: {missing_columns}"
        )

    transformed = dataframe.copy()

    transformed["order_date"] = pd.to_datetime(
        transformed["order_date"]
    )

    transformed["total"] = (
        transformed["quantity"]
        * transformed["unit_price"]
        * (1 - transformed["discount"])
    )

    transformed["total"] = transformed["total"].round(2)

    return transformed