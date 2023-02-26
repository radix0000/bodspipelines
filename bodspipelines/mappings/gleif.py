lei_properties = {'LEI': {'type': 'text'},
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

rr_propertties = {'Relationship': {'type': 'object', 
                                   'properties': {'StartNode': {'type': 'object', 
                                                                'properties': {'NodeID': {'type': 'text'}, 
                                                                               'NodeIDType': {'type': 'text'}}}, 
                                                  'EndNode': {'type': 'object', 
                                                              'properties': {'NodeID': {'type': 'text'}, 
                                                                             'NodeIDType': {'type': 'text'}}}, 
                                                  'RelationshipType': {'type': 'text'}, 
                                                  'RelationshipPeriods': {'type': 'text'}, 
                                                  'RelationshipStatus': {'type': 'text'}, 
                                                  'RelationshipQualifiers': {'type': 'text'}}}, 
                  'Registration': {'type': 'object', 
                                   'properties': {'InitialRegistrationDate': {'type': 'text'}, 
                                                  'LastUpdateDate': {'type': 'text'}, 
                                                  'RegistrationStatus': {'type': 'text'}, 
                                                  'NextRenewalDate': {'type': 'text'}, 
                                                  'ManagingLOU': {'type': 'text'}, 
                                                  'ValidationSources': {'type': 'text'}, 
                                                  'ValidationDocuments': {'type': 'text'}, 
                                                  'ValidationReference': {'type': 'text'}}}}

repex_properties = {'LEI': {'type': 'text'}, 
                    'ExceptionCategory': {'type': 'text'}, 
                    'ExceptionReason': {'type': 'text'}}

def match_lei(item):
    return {"LEI": item["LEI"]}

def match_rr(item):
    return {'Relationship': {'StartNode': {'NodeID': item['Relationship']['StartNode']['NodeID']}, 
                             'EndNode': {'NodeID': item['Relationship']['EndNode']['NodeID']},
                             'RelationshipType': item['Relationship']['RelationshipType']}

def match_repex(item):
    return {'LEI': item["LEI"], 'ExceptionCategory': item["ExceptionCategory"], 'ExceptionReason': item["ExceptionReason"]}
