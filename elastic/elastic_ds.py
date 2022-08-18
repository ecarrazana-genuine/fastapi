from elastic.connection import ElasticConnection


class ElasticQ:

    def __init__(self):
        self.es = ElasticConnection.get_connection()
        # self.elastic_api_url = "http://"+config.elastic_host + ":" + config.elastic_port + "/"
        self.df = None
        self.robot = None
        self.index = None

    def get_elastic_mapping(self):
        # mapping = requests.get(self.elastic_api_url+"_mapping")
        # return mapping.content
        return True

    def count(self, index, query):
        """Ejecuta una consulta que se pasa por parametro"""
        res = self.es.count(index=index, query=query)
        return res

    def execute_anyquery(self, index, query):
        """Ejecuta una consulta que se pasa por parametro"""
        res = self.es.search(index=index, query=query)
        return res['hits']['total'], res['hits']['hits']
