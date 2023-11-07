import os
from datetime import timedelta
from feast import FeatureStore, Entity, FeatureView, Feature, ValueType
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

"""
This script sets up the feature store for the 'performance' project using Feast.

It defines:
- An entity named 'example_id' corresponding to the primary key in the data source.
- A set of features that will be generated and stored in the PostgreSQL 'feature_data' table.
- A PostgreSQL data source that points to the 'feature_data' table.
- A feature view named 'example_feature_view' that references the defined features and entity.

Environment Variables:
- PG_USERNAME: The username for the PostgreSQL database.
- PG_PASSWORD: The password for the PostgreSQL database.

Usage:
1. Set the 'PG_USERNAME' and 'PG_PASSWORD' environment variables with your actual credentials.
2. Ensure that the PostgreSQL database and table are set up and match the configurations here.
3. Run this script in the root of your Feast feature repository to apply the definitions.

The script expects a table schema in the PostgreSQL database that aligns with the features being defined.
After running this script, the Feast feature store will be configured and ready for data ingestion and retrieval
as part of performance testing.
"""

# Retrieve database credentials from environment variables
user = os.getenv("PG_USERNAME")
password = os.getenv("PG_PASSWORD")
if not user or not password:
    raise EnvironmentError(
        "Please set the PG_USERNAME and PG_PASSWORD environment variables."
    )

# Configuration parameters
db_name = "performance"  # Database name
table_name = "feature_data"  # Table name where the data will be stored
host = "localhost"  # PostgreSQL host
port = "5432"  # PostgreSQL port
num_features = (
    20000  # Number of features to generate, align this with your actual generation
)

# Define an entity
example_id = Entity(
    name="example_id",
    value_type=ValueType.INT64,
    description="Example entity for feature view",
)

# Define features
features = [Feature(name=f"col{i}", dtype=ValueType.FLOAT) for i in range(num_features)]

# Define data source
feature_data_source = PostgreSQLSource(
    name="feature_data_source",
    table=table_name,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
    database=db_name,
    host=host,
    user=user,
    password=password,
    port=port,
)

# Define a feature view
example_feature_view = FeatureView(
    name="example_feature_view",
    entities=["example_id"],
    ttl=timedelta(days=1),
    features=features,
    batch_source=feature_data_source,
)

# Create a feature store object
store = FeatureStore(repo_path=".")

# Apply the definitions to the feature store
store.apply([example_id, example_feature_view])

print(
    f"Applied entity and feature view definitions to the Feast feature store at {store.repo_path}"
)
