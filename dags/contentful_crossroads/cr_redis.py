import redis
from airflow.models import Variable


class RedisClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.client = kwargs["client"]
        self.redis_client = redis.Redis.from_url(
            Variable.get(kwargs["client"] + "_redis")
        )
        self.entries_sync_token_key = (
            kwargs["client"] + "_contentful_entries_sync_token"
        )
        self.deletion_sync_token_key = (
            kwargs["client"] + "_contentful_deletion_sync_token"
        )

    def get_sync_token(self, type):
        if type == "deletion":
            return self.redis_client.get(self.deletion_sync_token_key)
        else:
            return self.redis_client.get(self.entries_sync_token_key)

    def save_sync_token(self, type, next_sync_token):
        if type == "deletion":
            return self.redis_client.set(self.deletion_sync_token_key, next_sync_token)
        else:
            return self.redis_client.set(self.entries_sync_token_key, next_sync_token)
