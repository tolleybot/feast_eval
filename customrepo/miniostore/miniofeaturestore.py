import pandas as pd
import pyarrow.parquet as pq
import s3fs
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
)
from feast.feature_view import FeatureView
from typing import List, Union
import logging
from feast.repo_config import RepoConfig

from pydantic import BaseModel


class MinioOfflineStoreConfig(BaseModel):
    type: str = "miniostore.miniofeaturestore.MinioOfflineStore"
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bucket_name: str


class MinioOfflineStore(OfflineStore):
    def __init__(self, repo_config: RepoConfig):
        super().__init__()
        # The configurations will be loaded from feature_store.yaml by Feast
        self.repo_config = repo_config
        self.bucket_name = self.repo_config.offline_store.bucket_name

    def get_historical_features(
        self,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
    ) -> RetrievalJob:
        """
        Retrieves historical features from the offline store based on the provided feature views,
        feature references, and entity dataframe.

        Parameters:
        feature_views (List[FeatureView]): A list of FeatureView objects. Each FeatureView provides
        necessary information about the features such as their source, transformation logic, and
        associated entities. These are used to identify the specific features to retrieve and join
        with the entity dataframe.

        feature_refs (List[str]): A list of feature references in the format "feature_view:feature".
        This list specifies the exact features to be retrieved. Each feature reference is a string
        that uniquely identifies a feature in the format of "feature_view_name:feature_name".
        Only features included in this list will be retrieved from the offline store.

        entity_df (Union[pd.DataFrame, str]): The entity dataframe or a query that produces it.
        This dataframe contains the entities and timestamps needed for retrieving historical features.
        It should have columns corresponding to the entity keys defined in the feature views and,
        optionally, a timestamp column to filter the feature data based on specific time ranges.

        Returns:
        RetrievalJob: An object that represents the result of the historical feature retrieval.
        This object can be used to materialize the final dataframe containing the joined entity
        and feature data.

        Raises:
        Exception: If any errors occur during the retrieval process, such as issues with connecting
        to the offline store, reading data, or performing the join operation.

        Notes:
        - The implementation of this method will vary depending on the specific offline store being used.
        - The method is expected to handle the joining of feature data with the entity dataframe
        based on the entities and timestamps.
        """
        try:
            # Setup MinIO connection using configurations from feature_store.yaml
            s3 = s3fs.S3FileSystem(
                client_kwargs={
                    "endpoint_url": self.repo_config.offline_store.minio_endpoint,
                    "aws_access_key_id": self.repo_config.offline_store.minio_access_key,
                    "aws_secret_access_key": self.repo_config.offline_store.minio_secret_key,
                }
            )

            # Adjust implementation based on your Parquet file structure and feature views
            bucket_name = feature_views[0].batch_source.path.split("/")[2]
            file_path = "/".join(feature_views[0].batch_source.path.split("/")[3:])
            full_path = f"{bucket_name}/{file_path}"

            # Reading the Parquet file from MinIO
            dataset = pq.ParquetDataset(full_path, filesystem=s3)
            table = dataset.read()
            feature_df = table.to_pandas()

            # Assuming single entity for simplicity, adjust as needed beyond testing
            join_key = feature_views[0].entities[0]

            # Combine features with entity dataframe (assuming appropriate join keys)
            result_df = pd.merge(entity_df, feature_df, on=join_key, how="left")

            # Wrap result DataFrame in a custom RetrievalJob
            return CustomRetrievalJob(result_df)
        except Exception as e:
            logging.error(f"Error in get_historical_features: {str(e)}")
            raise


class CustomRetrievalJob(RetrievalJob):
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def to_df(self):
        return self.dataframe

    def to_arrow(self):
        return self.dataframe.to_arrow()


# Remember to replace 'join_key' with the actual key used for joining.
