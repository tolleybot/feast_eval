import os
from datetime import timedelta
from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float32


def setup_feature_store(
    user: str,
    password: str,
    db_name: str = "performance",
    table_name: str = "feature_data",
    host: str = "localhost",
    port: str = "5432",
    num_features: int = 20000,
    ttl: timedelta = None,
    repo_path: str = ".",
):
    """
    Sets up the feature store for the 'performance' project using Feast.

    Parameters:
    - user: Database username
    - password: Database password
    - db_name: Database name
    - table_name: Table name where the data will be stored
    - host: PostgreSQL host
    - port: PostgreSQL port
    - num_features: Number of features to generate
    - ttl: Time to Live for the feature view
    - repo_path: Repository path for the feature store

    The function configures an entity, features, data source, and feature view,
    then applies them to the Feast feature store.
    """

    # Define an entity
    example_id = Entity(
        name="example_id",
        value_type=ValueType.INT64,
        description="Example entity for feature view",
    )

    schema = [Field(name=f"col{i}", dtype=Float32) for i in range(num_features)]

    # Define data source
    feature_data_source = PostgreSQLSource(
        name="feature_data_source",
        table=table_name,
        # Optionally, you can specify additional parameters like database or schema if needed
    )

    # Define a feature view for offline store and online store
    example_feature_view = FeatureView(
        name="offline_feature_view",
        entities=[example_id],
        schema=schema,
        online=True,  # Set True for online store to also be created
        source=feature_data_source,
        ttl=ttl,
    )

    # Create a feature store object
    store = FeatureStore(repo_path=repo_path)

    # Apply the definitions to the feature store
    store.apply([example_id, example_feature_view])

    print(
        f"Applied entity and feature view definitions to the Feast feature store at {store.repo_path}"
    )


# Usage
if __name__ == "__main__":
    # Retrieve database credentials from environment variables
    user = os.getenv("PG_USERNAME")
    password = os.getenv("PG_PASSWORD")
    if not user or not password:
        raise EnvironmentError(
            "Please set the PG_USERNAME and PG_PASSWORD environment variables."
        )

    # Call the function with the required parameters
    setup_feature_store(user, password, table_name="perform_large", num_features=500)
