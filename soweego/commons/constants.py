#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import TypeVar

from recordlinkage import NaiveBayesClassifier, SVMClassifier

from soweego.importer import models
from soweego.wikidata import vocabulary

# Miscellanea
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
OCCUPATION = 'occupations'
SUPPORTED_QUERY_TYPES = (CLASS, OCCUPATION)
SUPPORTED_QUERY_SELECTORS = (IDENTIFIER, LINKS, DATASET, METADATA)

# Entity types and corresponding Wikidata query
HANDLED_ENTITIES = {
    'band': CLASS,
    'actor': OCCUPATION,
    'director': OCCUPATION,
    'musician': OCCUPATION,
    'producer': OCCUPATION,
    'writer': OCCUPATION
}

# DB entity Python types for typed function signatures
DB_ENTITY = TypeVar('DB_ENTITY', models.base_entity.BaseEntity,
                    models.base_link_entity.BaseLinkEntity, models.base_nlp_entity.BaseNlpEntity)

# DB entities and their Wikidata class QID
TARGET_CATALOGS = {
    'discogs': {
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': models.discogs_entity.DiscogsMusicianEntity,
            'link_entity': models.discogs_entity.DiscogsMusicianLinkEntity,
            'nlp_entity': models.discogs_entity.DiscogsMusicianNlpEntity
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': models.discogs_entity.DiscogsGroupEntity,
            'link_entity': models.discogs_entity.DiscogsGroupLinkEntity,
            'nlp_entity': models.discogs_entity.DiscogsGroupNlpEntity
        }
    },
    'imdb': {
        'actor': {
            'qid': vocabulary.ACTOR,
            'entity': models.imdb_entity.ImdbActorEntity,
            'link_entity': None,
            'nlp_entity': None
        },
        'director': {
            'qid': vocabulary.FILM_DIRECTOR,
            'entity': models.imdb_entity.ImdbDirectorEntity,
            'link_entity': None,
            'nlp_entity': None
        },
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': models.imdb_entity.ImdbMusicianEntity,
            'link_entity': None,
            'nlp_entity': None
        },
        'producer': {
            'qid': vocabulary.FILM_PRODUCER,
            'entity': models.imdb_entity.ImdbProducerEntity,
            'link_entity': None,
            'nlp_entity': None
        },
        'writer': {
            'qid': vocabulary.SCREENWRITER,
            'entity': models.imdb_entity.ImdbWriterEntity,
            'link_entity': None,
            'nlp_entity': None
        }
    },
    'musicbrainz': {
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': models.musicbrainz_entity.MusicbrainzArtistEntity,
            'link_entity': models.musicbrainz_entity.MusicbrainzArtistLinkEntity,
            'nlp_entity': None
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': models.musicbrainz_entity.MusicbrainzBandEntity,
            'link_entity': models.musicbrainz_entity.MusicbrainzBandLinkEntity,
            'nlp_entity': None
        }
    }
}

# When building the wikidata dump for catalogs in this array
# also the QIDs of a person's occupations will be included
# as part of the dump
REQUIRE_OCCUPATIONS = [
    'imdb'
]


# Wikidata field & target column names
INTERNAL_ID = 'internal_id'
CATALOG_ID = 'catalog_id'
QID = 'qid'
TID = 'tid'
ALIAS = 'alias'
PSEUDONYM = 'pseudonym'
DATE_OF_BIRTH = vocabulary.LINKER_PIDS[vocabulary.DATE_OF_BIRTH]
DATE_OF_DEATH = vocabulary.LINKER_PIDS[vocabulary.DATE_OF_DEATH]
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

# File names
WD_TRAINING_SET = 'wikidata_%s_%s_training_set.jsonl.gz'
WD_CLASSIFICATION_SET = 'wikidata_%s_%s_classification_set.jsonl.gz'
SAMPLES = '%s_%s_%s_samples%02d.pkl.gz'
FEATURES = '%s_%s_%s_features%02d.pkl.gz'
LINKER_MODEL = '%s_%s_%s_model.pkl'
LINKER_RESULT = '%s_%s_%s_linker_result.csv.gz'
LINKER_EVALUATION_PREDICTIONS = '%s_%s_%s_linker_evaluation_predictions.csv.gz'
LINKER_PERFORMANCE = '%s_%s_%s_linker_performance.txt'

# Supervised classification
CLASSIFIERS = {
    'naive_bayes': NaiveBayesClassifier,
    'support_vector_machines': SVMClassifier,
    'nb': NaiveBayesClassifier,  # Shorthand
    'svm': SVMClassifier  # Shorthand
}
CLASSIFICATION_RETURN_SERIES = ('classification.return_type', 'series')
CONFIDENCE_THRESHOLD = 0.5


# precisions for the `pandas.Period` class.
# Listed from least to most precise, as defined here:
# http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
PD_PERIOD_PRECISIONS = [
    'A-DEC',  # we know only the year
    'M',  # we know up to the month
    'D',  # up to the day
    'H',  # up to the hour
    'T',  # up to the minute
    'S',  # up to the second
    'U',  # up to the microsecond
    'N',  # up to the nanosecond
]
