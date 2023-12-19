import polars as pl
import pyarrow.parquet as pq
import s3fs
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
)
from feast.infra.registry.registry import Registry
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig
from feast.data_source import DataSource

from typing import List, Union, Optional, Dict, Any
import logging
from pydantic import BaseModel
import os

s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


class PolarsOfflineStoreConfig(BaseModel):
    type: str = "polarsfeaturestore.PolarsOfflineStore"
    minio_endpoint: str
    bucket_name: str


class CustomRetrievalJob(RetrievalJob):
    def __init__(self, dataframe: pl.DataFrame):
        self.dataframe = dataframe

    def to_df(self):
        return self.dataframe

    def to_arrow(self):
        return self.dataframe.to_arrow()

    def _to_arrow_internal(self):
        # TODO: Implement this method
        pass

    def _to_df_internal(self):
        # TODO: Implement this method
        pass

    @property
    def full_feature_names(self):
        # TODO: Implement this property
        pass

    @property
    def metadata(self):
        # TODO: Implement this property
        pass

    @property
    def on_demand_feature_views(self):
        # TODO: Implement this property
        pass

    def persist(self):
        # TODO: Implement this method
        pass


class PolarsOfflineStore(OfflineStore):
    def __init__(self):
        super().__init__()

    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        entity_names: Optional[Dict[str, Any]],
        feature_view: FeatureView,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """
        Retrieve a full dataset from the specified data source.
        """
        try:
            s3 = s3fs.S3FileSystem(
                client_kwargs={
                    "endpoint_url": config.offline_store.minio_endpoint,
                    "aws_access_key_id": s3_access_key,
                    "aws_secret_access_key": s3_secret_key,
                }
            )

            # Construct the file path for the feature view data
            bucket_name = data_source.path.split("/")[2]
            file_path = "/".join(data_source.path.split("/")[3:])
            full_path = f"{bucket_name}/{file_path}"

            # Read the Parquet file
            with s3.open(full_path, "rb") as f:
                feature_df = pl.read_parquet(f)

            # Process and filter the data as necessary for your application
            # This might involve filtering based on entity_names, handling full_feature_names, etc.

            return CustomRetrievalJob(feature_df)

        except Exception as e:
            logging.error(f"Error in pull_all_from_table_or_query: {str(e)}")
            raise

    def pull_latest_from_table_or_query(
        self,
        config: RepoConfig,
        data_source: DataSource,
        entity_names: Optional[Dict[str, Any]],
        feature_view: FeatureView,
        timestamp_field: str,
        created_timestamp_column: Optional[str],
    ) -> RetrievalJob:
        """
        Retrieve the latest data from the specified data source.
        """
        try:
            s3 = s3fs.S3FileSystem(
                client_kwargs={
                    "endpoint_url": config.offline_store.minio_endpoint,
                    "aws_access_key_id": s3_access_key,
                    "aws_secret_access_key": s3_secret_key,
                }
            )

            # Construct the file path for the feature view data
            bucket_name = data_source.path.split("/")[2]
            file_path = "/".join(data_source.path.split("/")[3:])
            full_path = f"{bucket_name}/{file_path}"

            # Read the Parquet file
            with s3.open(full_path, "rb") as f:
                feature_df = pl.read_parquet(f)

            # Apply any necessary filters to get the latest data
            # Assuming your timestamp_field is a datetime column in your dataset
            # You might need to adjust this logic based on your specific data structure
            latest_feature_df = feature_df.sort(timestamp_field, reverse=True).unique(
                subset=entity_names.keys()
            )

            # Optionally, handle the created_timestamp_column
            # This would depend on how your data is structured and how you want to handle it

            return CustomRetrievalJob(latest_feature_df)

        except Exception as e:
            logging.error(f"Error in pull_latest_from_table_or_query: {str(e)}")
            raise

    # @staticmethod
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pl.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        try:
            s3 = s3fs.S3FileSystem(
                client_kwargs={
                    "endpoint_url": config.offline_store.minio_endpoint,
                    "aws_access_key_id": s3_access_key,
                    "aws_secret_access_key": s3_secret_key,
                }
            )

            combined_feature_df = pl.DataFrame()
            for fv in feature_views:
                # with s3.open(full_path, "rb") as f:
                feature_df = pl.read_parquet(fv.batch_source.path)

                selected_features = [
                    ref.split(":")[1]
                    for ref in feature_refs
                    if ref.startswith(fv.name + ":")
                ]

                if full_feature_names:
                    feature_df = feature_df.select([fv.entities[0]] + selected_features)
                    feature_df.columns = [
                        fv.name + "__" + col if col in selected_features else col
                        for col in feature_df.columns
                    ]
                else:
                    feature_df = feature_df.select([fv.entities[0]] + selected_features)

                if combined_feature_df.height == 0:
                    combined_feature_df = feature_df
                else:
                    combined_feature_df = combined_feature_df.join(
                        feature_df, on=fv.entities[0], how="outer"
                    )

            if isinstance(entity_df, pl.DataFrame):
                result_df = entity_df.join(
                    combined_feature_df, on=feature_views[0].entities[0], how="left"
                )
            else:
                raise NotImplementedError("SQL query handling is not implemented.")

            return CustomRetrievalJob(result_df)

        except Exception as e:
            logging.error(f"Error in get_historical_features: {str(e)}")
            raise
