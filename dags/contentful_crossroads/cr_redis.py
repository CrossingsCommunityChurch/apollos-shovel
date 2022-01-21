import redis
from airflow.models import Variable

redis_client = redis.Redis.from_url(Variable.get("crossroads_kids_club_redis"))

kc_redis_token = "crossroads_kids_club_contentful_sync_token"
