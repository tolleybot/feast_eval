import psycopg2
import numpy as np
import pandas as pd
from io import StringIO
import os

# Remember to port foward the PostgreSQL port to your local machine
# Example: kubectl port-forward svc/postgresql 5432:5432

# Retrieve database credentials from environment variables
user = os.getenv("PG_USERNAME")
password = os.getenv("PG_PASSWORD")
if not user or not password:
    raise EnvironmentError(
        "Please set the PG_USERNAME and PG_PASSWORD environment variables."
    )


def get_table_size_info(dbname, user, password, host, port, table_name):
    """
    Function to get the number of rows and columns of a specified table.

    Parameters:
    dbname (str): Database name
    user (str): Username for the database
    password (str): Password for the database
    host (str): Host where the database server is located
    port (int): Port number for the database connection
    table_name (str): Name of the table to inspect

    Returns:
    tuple: A tuple containing the number of rows and columns in the table
    """
    # Establish the database connection
    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )
    cur = conn.cursor()

    # Get the number of rows
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    num_rows = cur.fetchone()[0]

    # Get the number of columns
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND table_schema = 'public';
    """
    )
    num_columns = cur.fetchone()[0]

    # Close the cursor and the connection
    cur.close()
    conn.close()

    return num_rows, num_columns


if __name__ == "__main__":
    dbname = "performance"
    user = user
    password = password
    host = "localhost"
    port = 5432
    table_name = "perform_large"

    rows, columns = get_table_size_info(dbname, user, password, host, port, table_name)
    print(f"The table '{table_name}' contains {rows} rows and {columns} columns.")
