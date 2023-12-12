import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow._s3fs import S3FileSystem
from pyarrow.parquet import ParquetDataset

# Set up the S3 filesystem client
s3 = S3FileSystem()
# Uncomment and configure with your credentials and endpoint
# s3 = S3FileSystem(
#     key="accesskey",  # Replace with your MinIO access key
#     secret="secretkey",  # Replace with your MinIO secret key
#     endpoint_override="http://127.0.0.1:9000"  # Replace with your MinIO URL
# )

# Sample data to write
data = {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
df = pd.DataFrame(data)

# Convert DataFrame to Parquet Table
table = pa.Table.from_pandas(df)

# Write to MinIO
bucket_name = "my-bucket"
file_path = "test_data.parquet"
try:
    with s3.open_output_stream(f"{bucket_name}/{file_path}") as f:
        pq.write_table(table, f)
    print("File written successfully")
except Exception as e:
    print(f"Error writing file: {e}")

# Read from MinIO using ParquetDataset
try:
    dataset = ParquetDataset(f"{bucket_name}/{file_path}", filesystem=s3)
    table = dataset.read()
    print(table.to_pandas())
except Exception as e:
    print(f"Error reading file: {e}")
