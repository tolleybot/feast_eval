import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database():
    # Define connection parameters - replace with your values
    db_params = {
        "dbname": "postgres",
        "user": "your_user",
        "password": "your_password",
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


# Example usage
# create_database()
