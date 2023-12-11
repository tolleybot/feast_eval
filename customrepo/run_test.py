from feast import FeatureStore
from utils import (
    generate_feast_repository_definitions,
    create_parquet_file,
)
import time
import pandas as pd


def run_tests():
    # Test configurations
    test_cases = [(10, 100), (20, 200)]  # Define your test cases here

    for num_columns, num_rows in test_cases:
        # Initialize Feature Store
        minio_store = FeatureStore()

        # Define the path for the Parquet file in MinIO
        bucket_name = "my-bucket"
        s3_filepath = f"test_data_{num_columns}_{num_rows}.parquet"

        # Create and upload the Parquet file to MinIO
        create_parquet_file(
            num_columns, num_rows, bucket_name, minio_store.config, s3_filepath
        )

        # Generate Feast repository definitions
        parquet_file_path = f"s3://{bucket_name}/{s3_filepath}"
        defs = generate_feast_repository_definitions(num_columns, parquet_file_path)

        # Simulate applying Feast definitions (if needed for MinioOfflineStore setup)
        # ...

        # Create an entity DataFrame for historical feature retrieval
        entity_df = pd.DataFrame(
            {
                "id": list(range(num_rows)),
                "event_timestamp": [pd.Timestamp.now() for _ in range(num_rows)],
            }
        )

        # Time the historical feature retrieval
        # Assuming get_historical_features() is adjusted to work directly with MinioOfflineStore
        begin = time.perf_counter_ns()
        feature_df = minio_store.get_historical_features(
            feature_views=defs[-1],  # Assuming last definition is the FeatureView
            feature_refs=[
                f"dummy_feature_view:feature_{i}" for i in range(num_columns)
            ],
            entity_df=entity_df,
        )
        end = time.perf_counter_ns()

        elapsed_time_ms = (end - begin) / 1e6  # Convert nanoseconds to milliseconds
        print(
            f"Test with {num_columns} columns and {num_rows} rows took {elapsed_time_ms} ms"
        )


if __name__ == "__main__":
    run_tests()
