from locust import User, task, between
from feast import FeatureStore


class FeastUser(User):
    wait_time = between(1, 5)

    def on_start(self):
        self.store = FeatureStore(repo_path="/path/to/your/feature_repo")

    @task
    def get_historical_features(self):
        # Replace with your actual entity_df
        entity_df = ...
        self.store.get_historical_features(
            entity_df=entity_df,
            feature_refs=[...],
        )
