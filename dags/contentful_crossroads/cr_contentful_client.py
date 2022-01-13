from contentful import Client
from airflow.models import Variable

ContentfulClient = Client(
    "y3a9myzsdjan",
    Variable.get("crossroads_contentful_api_key"),
)
