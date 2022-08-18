from elastic.connection import ElasticConnection


class ElasticIDX:

    def __init__(self, index_name):
        self.es = ElasticConnection.get_connection()
        self.index_name = index_name

    def delete_index(self):
        res = None
        if self.es.indices.exists(index=self.index_name):
            res = self.es.indices.delete(index=self.index_name)
        return res

    def create_index(self, number_of_shards=2, number_of_replicas=0):
        mapping = {"logEvent":
                        {"properties":
                             {
                                "Component":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "Source":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "fileName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "fingerprint":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "jobId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "level":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "logType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "machineId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "machineName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "message":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "processName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "processVersion":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "robotName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "timeStamp":{"type":"date", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                                "totalExecutionTime": {"type": "text","fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
                                "totalExecutionTimeInSeconds": {"type": "long"},
                                "windowsIdentity":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}
                             }
                         }
                   }
        request_body = {"settings": {"number_of_shards": number_of_shards, "number_of_replicas": number_of_replicas},
                        'mappings': {}} # mapping

        res = None
        if not self.es.indices.exists(index=self.index_name):
            res = self.es.indices.create(index=self.index_name, body=request_body)
        return res

    def bulk_indexing(self, bulk_data):
        print("Inserting records to %s" % self.index_name, "\n")
        res = self.es.bulk(index=self.index_name, body=bulk_data, refresh=True, request_timeout=1000)
        return res

    def bulk_delete(self, bulk_data):
        print("Deleting records in: %s" % self.index_name, "\n")
        res = self.es.bulk(index=self.index_name, body=bulk_data, refresh=True, request_timeout=1000)
        return res

    def update_document(self, doc_id, doc):
        resp = self.es.update(index=self.index_name, id=doc_id, doc=doc)
        print(resp['result'])












