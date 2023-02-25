import os
import json
from elasticsearch import Elasticsearch

def create_client():
    """Create Elasticsearch client"""
    protocol = os.getenv('ELASTICSEARCH_PROTOCOL')
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    password = os.getenv('ELASTICSEARCH_PASSWORD')
    if password:
        return Elasticsearch(f"{protocol}://{host}:{port}", basic_auth=('elastic', password))
    else:
        return Elasticsearch(f"{protocol}://{host}:{port}") #, basic_auth=('elastic', password))

def index_definition(record, out):
    """Create index definition from record"""
    for key in record:
        if isinstance(record[key], dict):
            out2 = index_definition(record[key], {})
            out[key] = {"type":"object", "properties": out2}
        else:
            out[key] = {"type": "text"}
    return out

class ElasticsearchClient:
    """ElasticsearchClient class"""
    def __init__(self):
        self.client = create_client()
        self.index_name = None
        #self.index_count = 0

    def set_index(self, index_name):
        self.index_name = index_name

    def test(self):
        print(self.client.info())

    def create_index(self, index_name, properties):
        self.set_index(index_name)
        # index settings
        settings = {"number_of_shards": 1,
                    "number_of_replicas": 0}
        mappings = {"dynamic": "strict",
                    "properties": properties}
        if not self.client.indices.exists(index=self.index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            self.client.options(ignore_status=400).indices.create(index=self.index_name, settings=settings, mappings=mappings)
            print('Elasticserach created Index')

    def delete_index(self):
        self.client.options(ignore_status=[400, 404]).indices.delete(index=self.index_name)

    def store_record(self, record):
        self.index_count += 1
        return self.client.index(index=self.index_name, id=self.index_count, document=record)

    def store_data(self, data):
        if isinstance(data, list):
            for d in data:
                self.client.index(index=self.index_name, document=d)
        else:
            self.client.index(index=self.index_name, document=data)

    def search(self, search):
        return self.client.search(index=self.index_name, query=search)

    def list_indexes(self):
        return self.client.indices.get_alias(index="*")

    def get_mapping(self, index_name):
        return self.client.indices.get_mapping(index=index_name)

    def check_new(self, data):
        pass

#elasticsearch = ElasticsearchClient('http', 'localhost', '9200', 'fGSX0QA*8Ukn4lYQddu9', 'test_index')

#print(elasticsearch.test())

properties = {
                    "title": {
                        "type": "text"
                    },
                    "submitter": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "calories": {
                        "type": "integer"
                    },
                }

#elasticsearch.delete_index()
#elasticsearch.create_index(properties)
#print(elasticsearch.list_indexes())
#record = {"title": "Poo", "submitter": "Foo", "description": "Poo Poo", "calories": 11}
#print(elasticsearch.store_record(record))
#print(elasticsearch.search({'match': {'calories': '11'}}))
#print(elasticsearch.search({'match_all': {}}))

records = [{'LEI': '097900BICQ0000135514', 
          'Entity': {'LegalName': 'Ing. Magdaléna Beňo Frackowiak ZARIA TRAVEL', 
                     'TransliteratedOtherEntityNames': {'TransliteratedOtherEntityName': 'ING MAGDALENA BENO FRACKOWIAK ZARIA TRAVEL'}, 
                     'LegalAddress': {'FirstAddressLine': 'Partizánska Ľupča 708', 'City': 'Partizánska Ľupča', 'Country': 'SK', 'PostalCode': '032 15'}, 
                     'HeadquartersAddress': {'FirstAddressLine': 'Partizánska Ľupča 708', 'City': 'Partizánska Ľupča', 'Country': 'SK', 'PostalCode': '032 15'}, 
                     'RegistrationAuthority': {'RegistrationAuthorityID': 'RA000670', 'RegistrationAuthorityEntityID': '43846696'}, 
                     'LegalJurisdiction': 'SK', 
                     'EntityCategory': 'SOLE_PROPRIETOR', 
                     'LegalForm': {'EntityLegalFormCode': 'C4PZ'}, 
                     'EntityStatus': 'ACTIVE', 
                     'EntityCreationDate': '2007-11-15T08:00:00+01:00'}, 
          'Registration': {'InitialRegistrationDate': '2018-02-16T00:00:00+01:00', 
                           'LastUpdateDate': '2023-01-10T08:30:56.044+01:00', 
                           'RegistrationStatus': 'ISSUED', 
                           'NextRenewalDate': '2024-02-16T00:00:00+01:00', 
                           'ManagingLOU': '097900BEFH0000000217', 
                           'ValidationSources': 'FULLY_CORROBORATED', 
                           'ValidationAuthority': {'ValidationAuthorityID': 'RA000670', 'ValidationAuthorityEntityID': '43846696'}}},
{'LEI': '097900BICO0000139589', 
 'Entity': {'LegalName': 'PROFINCON, s.r.o.', 
            'TransliteratedOtherEntityNames': {'TransliteratedOtherEntityName': 'PROFINCON, SRO'}, 
            'LegalAddress': {'FirstAddressLine': 'Štvrtok 8', 'City': 'Štvrtok', 'Country': 'SK', 'PostalCode': '913 05'}, 
            'HeadquartersAddress': {'FirstAddressLine': 'Štvrtok 8', 'City': 'Štvrtok', 'Country': 'SK', 'PostalCode': '913 05'}, 
            'RegistrationAuthority': {'RegistrationAuthorityID': 'RA000526', 'RegistrationAuthorityEntityID': '46815465'}, 
            'LegalJurisdiction': 'SK', 
            'EntityCategory': 'GENERAL', 
            'LegalForm': {'EntityLegalFormCode': 'VSZS'}, 
            'EntityStatus': 'ACTIVE', 
            'EntityCreationDate': '2012-09-07T08:00:00+02:00'}, 'Registration': {'InitialRegistrationDate': '2018-02-14T00:00:00+01:00', 'LastUpdateDate': '2022-04-07T00:00:00+02:00', 'RegistrationStatus': 'ISSUED', 'NextRenewalDate': '2023-05-12T00:00:00+02:00', 'ManagingLOU': '097900BEFH0000000217', 'ValidationSources': 'FULLY_CORROBORATED', 'ValidationAuthority': {'ValidationAuthorityID': 'RA000526', 'ValidationAuthorityEntityID': '46815465'}}},
{'LEI': '097900BICP0000137988', 'Entity': {'LegalName': 'Energeticko-Chemický odborový zväz', 'TransliteratedOtherEntityNames': {'TransliteratedOtherEntityName': 'ENERGETICKO-CHEMICKY ODBOROVY ZVAZ'}, 'LegalAddress': {'FirstAddressLine': 'Osadná 6', 'City': 'Bratislava', 'Country': 'SK', 'PostalCode': '831 03'}, 'HeadquartersAddress': {'FirstAddressLine': 'Osadná 6', 'City': 'Bratislava', 'Country': 'SK', 'PostalCode': '831 03'}, 'RegistrationAuthority': {'RegistrationAuthorityID': 'RA000527', 'RegistrationAuthorityEntityID': '30843928'}, 'LegalJurisdiction': 'SK', 'EntityCategory': 'GENERAL', 'LegalForm': {'EntityLegalFormCode': 'RF3D'}, 'EntityStatus': 'ACTIVE'}, 'Registration': {'InitialRegistrationDate': '2018-02-15T00:00:00+01:00', 'LastUpdateDate': '2022-01-21T00:00:00+01:00', 'RegistrationStatus': 'ISSUED', 'NextRenewalDate': '2023-02-15T00:00:00+01:00', 'ManagingLOU': '097900BEFH0000000217', 'ValidationSources': 'PARTIALLY_CORROBORATED', 'ValidationAuthority': {'ValidationAuthorityID': 'RA000527', 'ValidationAuthorityEntityID': '30843928'}}}]

#properties = index_definition(records[-1], {})

properties = {'LEI': {'type': 'text'},
              'Entity': {'type': 'object',
                         'properties': {'LegalName': {'type': 'text'},
                                        'TransliteratedOtherEntityNames': {'type': 'object',
                                                                           'properties': {'TransliteratedOtherEntityName': {'type': 'text'}}},
                                        'LegalAddress': {'type': 'object',
                                                         'properties': {'FirstAddressLine': {'type': 'text'},
                                                                         'City': {'type': 'text'},
                                                                         'Country': {'type': 'text'},
                                                                         'PostalCode': {'type': 'text'}}},
                                        'HeadquartersAddress': {'type': 'object',
                                                                'properties': {'FirstAddressLine': {'type': 'text'},
                                                                               'City': {'type': 'text'},
                                                                               'Country': {'type': 'text'},
                                                                               'PostalCode': {'type': 'text'}}},
                                        'RegistrationAuthority': {'type': 'object',
                                                                  'properties': {'RegistrationAuthorityID': {'type': 'text'},
                                                                                 'RegistrationAuthorityEntityID': {'type': 'text'}}},
                                        'LegalJurisdiction': {'type': 'text'},
                                        'EntityCategory': {'type': 'text'},
                                        'EntityCreationDate': {'type': 'text'},
                                        'LegalForm': {'type': 'object',
                                                      'properties': {'EntityLegalFormCode': {'type': 'text'}}},
                                                                     'EntityStatus': {'type': 'text'}}},
              'Registration': {'type': 'object',
                               'properties': {'InitialRegistrationDate': {'type': 'text'},
                                              'LastUpdateDate': {'type': 'text'},
                                              'RegistrationStatus': {'type': 'text'},
                                              'NextRenewalDate': {'type': 'text'},
                                              'ManagingLOU': {'type': 'text'},
                                              'ValidationSources': {'type': 'text'},
                                              'ValidationAuthority': {'type': 'object',
                                                                      'properties': {'ValidationAuthorityID': {'type': 'text'},
                                                                                     'ValidationAuthorityEntityID': {'type': 'text'}}}}}}


#print(properties)

#elasticsearch.delete_index()
#elasticsearch.create_index(properties)
#print(elasticsearch.list_indexes())
#record = {"title": "Poo", "submitter": "Foo", "description": "Poo Poo", "calories": 11}
#print(elasticsearch.store_record(records))
#print(elasticsearch.store_data(records))
#print(elasticsearch.search({'match': {'calories': '11'}}))

#print(elasticsearch.search({'match_all': {}}))
#print(elasticsearch.search({"match": {"LEI": {"query" : "097900BICQ0000135514"}}}))
#print(elasticsearch.search({"match" : { "LEI": "097900BICQ0000135514" }}))
