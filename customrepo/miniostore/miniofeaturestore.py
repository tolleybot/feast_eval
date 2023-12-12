import pandas as pd
import pyarrow.parquet as pq
import s3fs
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
)
from feast.infra.registry.registry import Registry
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig

from typing import List, Union
import logging
from pydantic import BaseModel
import os

s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


class MinioOfflineStoreConfig(BaseModel):
    type: str = "miniostore.miniofeaturestore.MinioOfflineStore"
    minio_endpoint: str
    bucket_name: str


class CustomRetrievalJob(RetrievalJob):
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def to_df(self):
        return self.dataframe

    def to_arrow(self):
        return self.dataframe.to_arrow()


class MinioOfflineStore(OfflineStore):
    def __init__(self, repo_config: RepoConfig):
        super().__init__()
        # The configurations will be loaded from feature_store.yaml by Feast
        self.repo_config = repo_config
        self.bucket_name = self.repo_config.offline_store.bucket_name

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """
        Retrieves historical features from the offline store.
        """
        try:
            s3 = s3fs.S3FileSystem(
                client_kwargs={
                    "endpoint_url": config.offline_store.minio_endpoint,
                    "aws_access_key_id": s3_access_key,
                    "aws_secret_access_key": s3_secret_key,
                }
            )

            # Combine data from all feature views
            combined_feature_df = pd.DataFrame()
            for fv in feature_views:
                # Construct the file path for the feature view data
                bucket_name = fv.batch_source.path.split("/")[2]
                file_path = "/".join(fv.batch_source.path.split("/")[3:])
                full_path = f"{bucket_name}/{file_path}"

                # Read the Parquet file
                with s3.open(full_path, "rb") as f:
                    feature_df = pq.read_table(f).to_pandas()

                # Select only the required features from this feature view
                selected_features = [
                    ref.split(":")[1]
                    for ref in feature_refs
                    if ref.startswith(fv.name + ":")
                ]
                if full_feature_names:
                    feature_df = feature_df[[fv.entities[0]] + selected_features]
                    feature_df.columns = [
                        fv.name + "__" + col if col in selected_features else col
                        for col in feature_df.columns
                    ]
                else:
                    feature_df = feature_df[[fv.entities[0]] + selected_features]

                # Merge with combined feature dataframe
                if combined_feature_df.empty:
                    combined_feature_df = feature_df
                else:
                    combined_feature_df = combined_feature_df.merge(
                        feature_df, on=fv.entities[0], how="outer"
                    )

            # Merge entity DataFrame with combined feature DataFrame
            if isinstance(entity_df, pd.DataFrame):
                result_df = entity_df.merge(
                    combined_feature_df, on=feature_views[0].entities[0], how="left"
                )
            else:
                # If entity_df is a string (SQL query), additional logic to handle the query is needed
                raise NotImplementedError("SQL query handling is not implemented.")

            return CustomRetrievalJob(result_df)

        except Exception as e:
            logging.error(f"Error in get_historical_features: {str(e)}")
            raise


# Remember to replace 'join_key' with the actual key used for joining.
