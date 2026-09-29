import pandas as pd

from elt.extract_raw import get_engine


def extract_orders_after(last_order_id: int) -> pd.DataFrame:
    engine = get_engine()

    query = """
        SELECT *
        FROM orders
        WHERE order_id > %(last_order_id)s
        ORDER BY order_id
    """

    try:
        return pd.read_sql(
            query,
            engine,
            params={"last_order_id": last_order_id}
        )
    finally:
        engine.dispose()


def extract_order_details_after(last_order_id: int) -> pd.DataFrame:
    engine = get_engine()

    query = """
        SELECT *
        FROM order_details
        WHERE order_id > %(last_order_id)s
        ORDER BY order_id, product_id
    """

    try:
        return pd.read_sql(
            query,
            engine,
            params={"last_order_id": last_order_id}
        )
    finally:
        engine.dispose()