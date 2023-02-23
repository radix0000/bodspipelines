import os
import sys
import time
import pytest

#sys.path.append(os.getcwd())

from bodspipelines.infrastructure.storage import ElasticStorage

@pytest.fixture
def lei_properties():
    """Index properties for LEI-CDF v3.1 data"""
    return {'LEI': {'type': 'text'},
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


@pytest.fixture
def lei_item():
    """Example LEI-CDF v3.1 data"""
    return {'LEI': '097900BICQ0000135514',
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
                           'ValidationAuthority': {'ValidationAuthorityID': 'RA000670', 'ValidationAuthorityEntityID': '43846696'}}}

def test_lei_storage_new(lei_properties, lei_item):
    """Test storing LEI-CDF v3.1 data in elasticsearch"""
    storage = ElasticStorage(protocol='http', host='localhost', port='9200', password='fGSX0QA*8Ukn4lYQddu9', indexes={'test-lei2': lei_properties})
    storage.delete_all('test-lei2')
    assert storage.process(lei_item, 'test-lei2') == lei_item
    time.sleep(1)
    assert storage.process(lei_item, 'test-lei2') == False
