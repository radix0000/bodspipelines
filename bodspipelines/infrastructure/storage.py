from typing import List, Union, Optional
from dataclasses import dataclass

from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient

class ElasticStorage:
    """Elasticsearch storage definition class"""
    #name: str
    #protocol: str
    #host: str
    #port: str
    #password: str
    #indexes: dict
    #storage: Optional[ElasticsearchClient] = None
    #current_index: str = None
    def __init__(self, indexes):
        self.indexes = indexes
        self.storage = ElasticsearchClient()
        self.current_index = None

    def setup_indexes(self):
        for index_name in self.indexes:
            self.storage.create_index(index_name, self.indexes[index_name])

    def list_indexes(self):
        return self.storage.list_indexes()

    def list_index_details(self, index_name):
        return self.storage.get_mapping(index_name)

    #def __post_init__(self):
    #    self.storage = ElasticsearchClient()
    #    self.setup_indexes()

    def set_index(self, index_name):
        self.storage.set_index(index_name)

    def delete_all(self, index_name):
        self.storage.set_index(index_name)
        self.storage.delete_index()
        self.storage.create_index(index_name, self.indexes[index_name])

    def add_item(self, item, item_type):
        query = {"match": {"LEI": {"query" : item["LEI"]}}}
        print(query)
        match = self.storage.search(query)
        print(match)
        if not match['hits']['hits']:
            out = self.storage.store_data(item)
            print(out)
            return item
        else:
            return False

    def process(self, item, item_type):
        if item_type != self.current_index:
            self.set_index(item_type)
        return self.add_item(item, item_type)

#storage = ElasticStorage(protocol='http', host='localhost', port='9200', password='fGSX0QA*8Ukn4lYQddu9', indexes=['lei','rr','repex'])

#print(storage.process(item, item_type))
#print(storage.process(item, item_type))
