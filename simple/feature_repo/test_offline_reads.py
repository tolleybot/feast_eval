from locust import User, task, between, events, run_single_user
from feast import FeatureStore
import os
import pandas as pd
import psycopg2
import time
import sys

# import locust.stats

# locust.stats.CSV_STATS_INTERVAL_SEC = 10  # default is 1 second
# # Determines how often the data is flushed to disk, default is 10 seconds
# locust.stats.CSV_STATS_FLUSH_INTERVAL_SEC = 60


class FeastUser(User):
    wait_time = between(1, 1)  # Task execution wait time between 1 to 5 seconds

    def on_start(self):
        # Initialize the Feast FeatureStore
        self.store = FeatureStore(repo_path=os.getenv("FEAST_REPO_PATH", "."))
        self.num_features = os.getenv("NUM_FEATURES", 500)
        self.rows = os.getenv("ROWS", 4096)
        self.feature_view_name = os.getenv("FEATURE_VIEW_NAME", "offline_feature_view")

    def load_full_dataframe(self, limit=4096):
        # Retrieve database credentials from env variables
        user = self.store.config.offline_store.user
        password = self.store.config.offline_store.password
        host = self.store.config.offline_store.host
        port = self.store.config.offline_store.port
        dbname = self.store.config.offline_store.database

        # Set up the connection string
        conn_str = f"dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'"

        # Assuming 'perform_large' is the table name where your data is stored
        table_name = os.getenv("TABLE_NAME", "perform_large")

        # Connect to the database
        conn = psycopg2.connect(conn_str)

        # Use pandas to load the data into a DataFrame
        query = (
            f"SELECT A.example_id, A.event_timestamp, "
            + ", ".join([f"A.col_{i+1}" for i in range(self.num_features)])
            + f" FROM {table_name} A LIMIT {limit}"
        )

        entity_df = pd.read_sql(query, conn)

        # Close the database connection
        conn.close()

        return entity_df

    @task
    def get_historical_features(self):
        # Use the feature view name and the names of the columns you want to retrieve
        features = [
            f"{self.feature_view_name}:col_{i+1}" for i in range(self.num_features)
        ]

        start_time = time.perf_counter()

        # Load the full dataframe from the 'perform_large' table
        self.entity_df = self.load_full_dataframe(limit=self.rows)

        try:
            # Retrieve historical features
            historical_features = self.store.get_historical_features(
                entity_df=self.entity_df,
                features=features,
                full_feature_names=True,
            )

            df = historical_features.to_df()

            # Log DataFrame info
            data_size = sys.getsizeof(df)
            row_count = len(df)
            col_count = len(df.columns)

            # If successful, fire a success event
            total_time = time.perf_counter() - start_time
            events.request.fire(
                request_type="Feast",
                name="get_historical_features",
                response_time=total_time,
                response_length=data_size,
                context={"rows": row_count, "cols": col_count},
                exception=None,
            )

            print(
                f"Data size: {data_size} bytes, Rows: {row_count}, Columns: {col_count}"
            )

        except Exception as e:
            total_time = time.perf_counter() - start_time
            events.request.fire(
                request_type="Feast",
                name="get_historical_features",
                response_time=total_time,
                response_length=0,
                context={},
                exception=e,
            )

        print("Finished retrieving historical features.")


def main():
    # Set up environment and runner
    user = FeastUser(environment=run_single_user(FeastUser))

    # Start a Locust test with a single user
    user.run()

    # Debugging: Print statistics
    print(f"Requests: {user.environment.stats.num_requests}")
    print(f"Failures: {user.environment.stats.num_failures}")


if __name__ == "__main__":
    main()
