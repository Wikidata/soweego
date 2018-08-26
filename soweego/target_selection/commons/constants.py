#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from .utils import file_utils

"""target_selection constants"""

root_module_path = file_utils.get_module_path()
current_module_path = '{0}\\soweego\\target_selection'.format(root_module_path)

dump_states = '{0}\\resources\\dump_states.json'.format(current_module_path)
db_credentials = '{0}\\resources\\db_credentials.json'.format(current_module_path)
cofigs_path = '{0}\\resources\\configs.json'.format(current_module_path)

# Keys
last_modified_key = 'last-modified'

# wikidata constants
wikidata_samples = 'C:\\Code\Wikidata.Soweego\\soweego\\wikidata\\resources\\wikidata_sample.json'

# bibsys constants

bibsys_dictionary = 'bibsys_dictionary.json'
bibsys_references = 'bibsys_references.json'
bibsys_schema = 'bibsys_schema.json'

# keys
environment_key = 'ENVIRONMENT'
prod_db_key = 'PROD_DB' 
test_db_key = 'TEST_DB'

db_engine_key = 'DB_ENGINE'
user_key = 'USER'
password_key = 'PASSWORD'
host_key = 'HOST'

create_db_schema_key = "CREATE_DB_SCHEMA"
seed_db_schema_key = "SEED_DB"

# Environments
development = 'DEVELOPMENT'
production = 'PRODUCTION'

# DB

db_name = 'soweego'

output_folder = 'output'
resource_folder = 'resource'