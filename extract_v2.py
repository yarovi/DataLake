import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


def extract_sales() -> pd.DataFrame:

    database_url = (
        f"postgresql+psycopg://"
        f"{os.getenv('PYTHON_DB_USER')}:"
        f"{os.getenv('PYTHON_DB_PASSWORD')}@"
        f"{os.getenv('PYTHON_DB_HOST')}:"
        f"{os.getenv('PYTHON_DB_PORT')}/"
        f"{os.getenv('PYTHON_DB_NAME')}"
    )

    engine = create_engine(database_url)

    query = """
        SELECT
            o.order_id,
            o.order_date,
            c.company_name AS customer,
            e.first_name || ' ' || e.last_name AS employee,
            p.product_name,
            od.quantity,
            od.unit_price,
            od.discount
        FROM orders o
        JOIN customers c
            ON c.customer_id = o.customer_id
        JOIN employees e
            ON e.employee_id = o.employee_id
        JOIN order_details od
            ON od.order_id = o.order_id
        JOIN products p
            ON p.product_id = od.product_id
        ORDER BY o.order_id
    """

    dataframe = pd.read_sql(query, engine)

    engine.dispose()

    return dataframe