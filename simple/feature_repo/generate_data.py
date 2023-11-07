import psycopg2
import numpy as np
import pandas as pd
from io import StringIO
import os
from datetime import datetime

# Retrieve database credentials from environment variables
user = os.getenv("PG_USERNAME")
password = os.getenv("PG_PASSWORD")
if not user or not password:
    raise EnvironmentError(
        "Please set the PG_USERNAME and PG_PASSWORD environment variables."
    )
# Remember to port foward the PostgreSQL port to your local machine
# Example: kubectl port-forward svc/postgresql 5432:5432


def create_table(cur, table_name, num_features):
    # Drop the table if it exists
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name}"
    cur.execute(drop_table_sql)

    # Construct the SQL for table creation
    columns = ", ".join([f"col{i} FLOAT" for i in range(num_features)])
    create_table_sql = (
        f"CREATE TABLE {table_name} ("
        f"id SERIAL PRIMARY KEY, event_timestamp TIMESTAMP, {columns})"
    )
    cur.execute(create_table_sql)


# Define the function to generate and insert data into PostgreSQL
def generate_data(db_params, table_name, num_rows, num_features):
    # Generate random float data
    data = np.random.rand(num_rows, num_features)

    # Convert to a Pandas DataFrame
    df = pd.DataFrame(data, columns=[f"col{i}" for i in range(num_features)])
    # Add a timestamp column with the current time for all rows
    df["event_timestamp"] = pd.Timestamp(datetime.now())

    # Calculate the size of the data in megabytes
    data_size_bytes = df.memory_usage(index=True).sum()
    data_size_mb = data_size_bytes / (1024 * 1024)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create table if it doesn't exist
    create_table(cur, table_name, num_features)
    conn.commit()

    # Create a buffer for the data
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    # Columns to insert, excluding 'id' which is auto-generated
    columns = [f"col{i}" for i in range(num_features)] + ["event_timestamp"]

    # Insert the data into the database
    try:
        cur.copy_from(buffer, table_name, sep=",", columns=columns)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    print("Data inserted using COPY FROM successfully.")

    # Close the connection
    cur.close()
    conn.close()

    # Return the size of the data and the DataFrame for Feast
    return data_size_mb, df


if __name__ == "__main__":
    # Example usage:
    db_params = {
        "dbname": "performance",
        "user": user,
        "password": password,
        "host": "localhost",
        "port": 5432,
    }
    table_name = "perform_large"
    num_rows = 4096 * 10
    # num_features = 20000 # I Hit the max row size limit of 8KB
    num_features = 500

    # Generate and insert data
    data_size_mb, entity_df = generate_data(
        db_params, table_name, num_rows, num_features
    )

    print(f"Data size: {data_size_mb} MB")
    entity_df.head()
