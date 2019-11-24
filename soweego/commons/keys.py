#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constant keys"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

# Supported catalogs
DISCOGS = 'discogs'
IMDB = 'imdb'
MUSICBRAINZ = 'musicbrainz'
TWITTER = 'twitter'

# Supported entities
# People
ACTOR = 'actor'
BAND = 'band'
DIRECTOR = 'director'
PRODUCER = 'producer'
MUSICIAN = 'musician'
WRITER = 'writer'
# Works
AUDIOVISUAL_WORK = 'audiovisual_work'
MUSICAL_WORK = 'musical_work'
WORK_TYPE = 'work_type'

# Keys of helper dictionaries
CATALOG_QID = 'catalog_qid'
CLASS_QID = 'class_qid'
PERSON_PID = 'person_pid'
WORK_PID = 'work_pid'
MAIN_ENTITY = 'main_entity'
LINK_ENTITY = 'link_entity'
NLP_ENTITY = 'nlp_entity'
RELATIONSHIP_ENTITY = 'relationship_entity'

# Web API bot login credentials
WIKIDATA_API_USER = 'WIKIDATA_API_USER'
WIKIDATA_API_PASSWORD = 'WIKIDATA_API_PASSWORD'

# Importer & internal DB
LAST_MODIFIED = 'last-modified'
PROD_DB = 'PROD_DB'
TEST_DB = 'TEST_DB'
DB_ENGINE = 'DB_ENGINE'
USER = 'USER'
PASSWORD = 'PASSWORD'
HOST = 'HOST'

# Validator
IDENTIFIER = 'identifier'
LINKS = 'links'
DATASET = 'dataset'
BIODATA = 'biodata'
FEMALE = 'female'
MALE = 'male'

# SPARQL queries
CLASS_QUERY = 'class_query'
OCCUPATION_QUERY = 'occupation_query'

# Wikidata & target pandas.DataFrame column names
CONFIDENCE = 'confidence'
QID = 'qid'
INTERNAL_ID = 'internal_id'
CATALOG_ID = 'catalog_id'
TID = 'tid'
ALIAS = 'alias'
SEX_OR_GENDER = 'sex_or_gender'
PLACE_OF_BIRTH = 'place_of_birth'
PLACE_OF_DEATH = 'place_of_death'
OCCUPATIONS = 'occupations'
GENRES = 'genres'
BIRTH_NAME = 'birth_name'
FAMILY_NAME = 'family_name'
GIVEN_NAME = 'given_name'
PSEUDONYM = 'pseudonym'
DATE_OF_BIRTH = 'born'  # Consistent with BaseEntity.born
DATE_OF_DEATH = 'died'  # Consistent with BaseEntity.died
# Consistent with BaseEntity
NAME = 'name'
NAME_TOKENS = 'name_tokens'
BIRTH_PRECISION = 'born_precision'
DEATH_PRECISION = 'died_precision'
# Consistent with BaseLinkEntity
URL = 'url'
URL_TOKENS = 'url_tokens'
# Consistent with BaseNlpEntity
DESCRIPTION = 'description'
DESCRIPTION_TOKENS = 'description_tokens'
# Target-specific pandas.DataFrame column names
REAL_NAME = 'real_name'

# Supervised classification
NAIVE_BAYES = 'naive_bayes'
LOGISTIC_REGRESSION = 'logistic_regression'
LINEAR_SVM = 'linear_support_vector_machines'
SVM = 'support_vector_machines'
RANDOM_FOREST = 'random_forest'
SINGLE_LAYER_PERCEPTRON = 'single_layer_perceptron'
MULTI_LAYER_PERCEPTRON = 'multi_layer_perceptron'

# For ensembles
VOTING_CLASSIFIER = 'voting_classifier'
GATED_CLASSIFIER = 'gated_classifier'
STACKED_CLASSIFIER = 'stacked_classifier'
