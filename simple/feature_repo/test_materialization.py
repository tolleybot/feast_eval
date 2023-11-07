from locust import User, task, between
from feast import FeatureStore
from datetime import datetime, timedelta


class FeastUser(User):
    wait_time = between(1, 5)

    def on_start(self):
        self.store = FeatureStore(repo_path="/path/to/your/feature_repo")

    @task
    def materialize_data(self):
        self.store.materialize(
            start_date=datetime.utcnow() - timedelta(days=1), end_date=datetime.utcnow()
        )
