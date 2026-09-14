import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


def get_engine():
    database_url = (
        f"postgresql+psycopg://"
        f"{os.getenv('PYTHON_DB_USER')}:"
        f"{os.getenv('PYTHON_DB_PASSWORD')}@"
        f"{os.getenv('PYTHON_DB_HOST')}:"
        f"{os.getenv('PYTHON_DB_PORT')}/"
        f"{os.getenv('PYTHON_DB_NAME')}"
    )

    return create_engine(database_url)


def extract_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()

    query = f"SELECT * FROM {table_name}"

    dataframe = pd.read_sql(query, engine)

    engine.dispose()

    return dataframe