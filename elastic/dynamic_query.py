
class Elastic_query:

    @staticmethod
    def query_get_job():
        query = \
            {
                "query": {
                    "nested": {
                        "path": "_source",
                        "query":
                            {
                                {"match": {"Id": "57f1fafa-30b3-485c-9314-13df1886bea1"}}
                            }
                    }
                }
            }
        return query
