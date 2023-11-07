import psycopg2
import numpy as np
import pandas as pd
from io import StringIO


# Define the function to generate and insert data into PostgreSQL
def generate_data(db_params, table_name, num_rows, num_features):
    # Generate random float data
    data = np.random.rand(num_rows, num_features)

    # Convert to a Pandas DataFrame
    df = pd.DataFrame(data, columns=[f"col{i}" for i in range(num_features)])

    # Calculate the size of the data in megabytes
    data_size_bytes = df.memory_usage(index=True).sum()
    data_size_mb = data_size_bytes / (1024 * 1024)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create a buffer for the data
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    # Insert the data into the database
    try:
        cur.copy_from(
            buffer, table_name, sep=",", null="\\N", columns=df.columns.tolist()
        )
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


# Example usage:
db_params = {
    "dbname": "your_db_name",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": 5432,
}
table_name = "your_table_name"
num_rows = 4096
num_features = 20000

# Generate and insert data
data_size_mb, entity_df = generate_data(db_params, table_name, num_rows, num_features)
