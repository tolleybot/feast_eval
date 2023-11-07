import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Retrieve database credentials from environment variables
user = os.getenv("PG_USERNAME")
password = os.getenv("PG_PASSWORD")
if not user or not password:
    raise EnvironmentError(
        "Please set the PG_USERNAME and PG_PASSWORD environment variables."
    )

# Remember to port foward the PostgreSQL port to your local machine
# Example: kubectl port-forward svc/postgresql 5432:5432


def create_database():
    # Define connection parameters - replace with your values
    db_params = {
        "dbname": "postgres",
        "user": user,
        "password": password,
        "host": "localhost",
    }

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Create database
    cursor.execute("CREATE DATABASE performance;")
    cursor.close()
    conn.close()

    print("Database 'performance' created successfully.")


if __name__ == "__main__":
    create_database()
