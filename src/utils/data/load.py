"""
    This module contains auxiliary functions for direct data loading.
"""
import pandas as pd
import sqlite3


def load_table(table_name: str, connection: sqlite3.Connection
               ) -> pd.DataFrame:
    """
    Load a table from the database and return it as a pandas dataframe

    Args:
        table_name (str): Name of the table to load
        connection (sqlite3.Connection): Connection to the database
    Returns:
        pandas.DataFrame: Dataframe with the data from the table
    """
    # read sql query into dataframe with column "id" as index
    df = pd.read_sql_query(
        f"SELECT * FROM {table_name}", connection, index_col="id")

    return df

