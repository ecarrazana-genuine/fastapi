from elasticsearch import Elasticsearch
from settings.config import ELASTIC_PASSWORD, CLOUD_ID


class ElasticConnection:

    @staticmethod
    def get_connection():

        # Create the client instance
        client = Elasticsearch(
            cloud_id=CLOUD_ID,
            basic_auth=("elastic", ELASTIC_PASSWORD)
        )

        return client



