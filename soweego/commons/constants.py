#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from recordlinkage import NaiveBayesClassifier, SVMClassifier

from soweego.importer.models import discogs_entity, musicbrainz_entity
from soweego.wikidata import vocabulary

# Keys
LAST_MODIFIED = 'last-modified'

PROD_DB = 'PROD_DB'
TEST_DB = 'TEST_DB'

DB_ENGINE = 'DB_ENGINE'
USER = 'USER'
PASSWORD = 'PASSWORD'
HOST = 'HOST'

IDENTIFIER = 'identifier'
LINKS = 'links'
DATASET = 'dataset'
METADATA = 'metadata'

# SPARQL queries
CLASS = 'class'
OCCUPATION = 'occupation'
SUPPORTED_QUERY_TYPES = (CLASS, OCCUPATION)
SUPPORTED_QUERY_SELECTORS = (IDENTIFIER, LINKS, DATASET, METADATA)

# Entity types and corresponding Wikidata query
HANDLED_ENTITIES = {
    'band': CLASS,
    'musician': OCCUPATION,
    'actor': OCCUPATION,
    'director': OCCUPATION,
    'producer': OCCUPATION
}

# TODO add IMDb entities
# DB entities and their Wikidata class QID
TARGET_CATALOGS = {
    'discogs': {
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': discogs_entity.DiscogsMusicianEntity,
            'link_entity': discogs_entity.DiscogsMusicianLinkEntity,
            'nlp_entity': discogs_entity.DiscogsMusicianNlpEntity
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': discogs_entity.DiscogsGroupEntity,
            'link_entity': discogs_entity.DiscogsGroupLinkEntity,
            'nlp_entity': discogs_entity.DiscogsGroupNlpEntity
        }
    },
    'imdb': {
        'actor': {
            'qid': vocabulary.ACTOR,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        },
        'director': {
            'qid': vocabulary.FILM_DIRECTOR,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        },
        'producer': {
            'qid': vocabulary.FILM_PRODUCER,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        }
    },
    'musicbrainz': {
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': musicbrainz_entity.MusicbrainzArtistEntity,
            'link_entity': musicbrainz_entity.MusicbrainzArtistLinkEntity,
            'nlp_entity': None
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': musicbrainz_entity.MusicbrainzBandEntity,
            'link_entity': musicbrainz_entity.MusicbrainzBandLinkEntity,
            'nlp_entity': None
        }
    }
}

# Wikidata & target field names
QID = 'qid'
TID = 'tid'
LABEL = 'label'
ALIAS = 'alias'
PSEUDONYM = 'pseudonym'
DESCRIPTION = 'description'
URL = 'url'

# File names
WD_DATASET_IO = 'wikidata_%s_dataset.jsonl.gz'
TARGET_DATASET_IO = '%s_dataset.jsonl.gz'
WD_DATASET_DATAFRAME_OUT = 'wikidata_%s_dataset.pkl.gz'
WD_TRAINING = 'wikidata_%s_training_set.jsonl.gz'
TARGET_TRAINING = '%s_training_set.jsonl.gz'
LINKER_MODEL = '%s_%s_model.pkl'
LINKER_RESULT = '%s_linker_result.jsonl.gz'

# Supervised classification algorithms
CLASSIFIERS = {
    'naive_bayes': NaiveBayesClassifier,
    'support_vector_machines': SVMClassifier,
    'nb': NaiveBayesClassifier,  # Shorthand
    'svm': SVMClassifier  # Shorthand
}
