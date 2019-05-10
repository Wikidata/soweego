#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import TypeVar

from soweego.importer import models
from soweego.wikidata import vocabulary

# Supported catalogs
DISCOGS = 'discogs'
IMDB = 'imdb'
MUSICBRAINZ = 'musicbrainz'

# Supported entities
ACTOR = 'actor'
BAND = 'band'
DIRECTOR = 'director'
MUSICIAN = 'musician'
PRODUCER = 'producer'
WRITER = 'writer'

# Miscellaneous keys
QID = 'qid'
PERSON_PID = 'person_pid'
WORK_PID = 'work_pid'

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

# Entities and corresponding Wikidata query
HANDLED_ENTITIES = {
    ACTOR: OCCUPATION,
    BAND: CLASS,
    DIRECTOR: OCCUPATION,
    MUSICIAN: OCCUPATION,
    PRODUCER: OCCUPATION,
    WRITER: OCCUPATION
}

# Target catalogs imported into the internal DB
# DB entity Python types for typed function signatures
DB_ENTITY = TypeVar('DB_ENTITY', models.base_entity.BaseEntity,
                    models.base_link_entity.BaseLinkEntity, models.base_nlp_entity.BaseNlpEntity)

# DB entities and their Wikidata class QID
MAIN_ENTITY = 'main_entity'
LINK_ENTITY = 'link_entity'
NLP_ENTITY = 'nlp_entity'
RELATIONSHIP_ENTITY = 'relationship_entity'
TARGET_CATALOGS = {
    DISCOGS: {
        MUSICIAN: {
            QID: vocabulary.MUSICIAN,
            MAIN_ENTITY: models.discogs_entity.DiscogsMusicianEntity,
            LINK_ENTITY: models.discogs_entity.DiscogsMusicianLinkEntity,
            NLP_ENTITY: models.discogs_entity.DiscogsMusicianNlpEntity,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        },
        BAND: {
            QID: vocabulary.BAND,
            MAIN_ENTITY: models.discogs_entity.DiscogsGroupEntity,
            LINK_ENTITY: models.discogs_entity.DiscogsGroupLinkEntity,
            NLP_ENTITY: models.discogs_entity.DiscogsGroupNlpEntity,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        }
    },
    IMDB: {
        ACTOR: {
            QID: vocabulary.ACTOR,
            MAIN_ENTITY: models.imdb_entity.ImdbActorEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        DIRECTOR: {
            QID: vocabulary.FILM_DIRECTOR,
            MAIN_ENTITY: models.imdb_entity.ImdbDirectorEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        MUSICIAN: {
            QID: vocabulary.MUSICIAN,
            MAIN_ENTITY: models.imdb_entity.ImdbMusicianEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        PRODUCER: {
            QID: vocabulary.FILM_PRODUCER,
            MAIN_ENTITY: models.imdb_entity.ImdbProducerEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        WRITER: {
            QID: vocabulary.SCREENWRITER,
            MAIN_ENTITY: models.imdb_entity.ImdbWriterEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        }
    },
    MUSICBRAINZ: {
        MUSICIAN: {
            QID: vocabulary.MUSICIAN,
            MAIN_ENTITY: models.musicbrainz_entity.MusicbrainzArtistEntity,
            LINK_ENTITY: models.musicbrainz_entity.MusicbrainzArtistLinkEntity,
            NLP_ENTITY: None,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        },
        BAND: {
            QID: vocabulary.BAND,
            MAIN_ENTITY: models.musicbrainz_entity.MusicbrainzBandEntity,
            LINK_ENTITY: models.musicbrainz_entity.MusicbrainzBandLinkEntity,
            NLP_ENTITY: None,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        }
    }
}

# When building the wikidata dump for catalogs in this array
# also the QIDs of a person's occupations will be included
# as part of the dump
REQUIRE_OCCUPATIONS = [
    IMDB
]

# Wikidata field & target column names
INTERNAL_ID = 'internal_id'
CATALOG_ID = 'catalog_id'
TID = 'tid'
ALIAS = 'alias'
BIRTH_NAME = vocabulary.LINKER_PIDS[vocabulary.BIRTH_NAME]
FAMILY_NAME = vocabulary.LINKER_PIDS[vocabulary.FAMILY_NAME]
GIVEN_NAME = vocabulary.LINKER_PIDS[vocabulary.GIVEN_NAME]
PSEUDONYM = vocabulary.LINKER_PIDS[vocabulary.PSEUDONYM]
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
# Target-specific column names
REAL_NAME = 'real_name'
# Cluster of fields with names
NAME_FIELDS = (NAME, ALIAS, BIRTH_NAME, FAMILY_NAME,
               GIVEN_NAME, PSEUDONYM, REAL_NAME)

# File names
WD_TRAINING_SET = 'wikidata_%s_%s_training_set.jsonl.gz'
WD_CLASSIFICATION_SET = 'wikidata_%s_%s_classification_set.jsonl.gz'
SAMPLES = '%s_%s_%s_samples%02d.pkl.gz'
FEATURES = '%s_%s_%s_features%02d.pkl.gz'
LINKER_MODEL = '%s_%s_%s_model.pkl'
LINKER_RESULT = '%s_%s_%s_linker_result.csv.gz'
LINKER_EVALUATION_PREDICTIONS = '%s_%s_%s_linker_evaluation_predictions.csv.gz'
LINKER_PERFORMANCE = '%s_%s_%s_linker_performance.txt'
NEURAL_NETWORK_CHECKPOINT_MODEL = '%s_best_checkpoint_model.hdf5'
COMPLETE_FEATURE_VECTORS = '%s_%s_%s_complete_feature_vectors.pkl.gz'
COMPLETE_WIKIDATA_CHUNKS = '%s_%s_%s_complete_wikidata_chunks.pkl.gz'
COMPLETE_TARGET_CHUNKS = '%s_%s_%s_complete_target_chunks.pkl.gz'
COMPLETE_POSITIVE_SAMPLES_INDEX = '%s_%s_%s_complete_positive_samples_index.pkl.gz'
WIKIDATA_API_SESSION = 'wiki_api_session.pkl'
SHARED_FOLDER = '/app/shared/'

# Supervised classification
SVC_CLASSIFIER = 'support_vector_machines'
LINEAR_SVC_CLASSIFIER = 'linear_support_vector_machines'
NAIVE_BAYES_CLASSIFIER = 'naive_bayes'
PERCEPTRON_CLASSIFIER = 'single_layer_perceptron'
MULTILAYER_CLASSIFIER = 'multi_layer_perceptron'

CLASSIFIERS = {
    'naive_bayes': NAIVE_BAYES_CLASSIFIER,
    'support_vector_machines': SVC_CLASSIFIER,
    'linear_support_vector_machines': LINEAR_SVC_CLASSIFIER,
    'single_layer_perceptron': PERCEPTRON_CLASSIFIER,
    'multi_layer_perceptron': MULTILAYER_CLASSIFIER,
    'nb': NAIVE_BAYES_CLASSIFIER,  # Shorthand
    'svm': SVC_CLASSIFIER,  # Shorthand
    'lsvm': LINEAR_SVC_CLASSIFIER,  # Shorthand
    'slp': PERCEPTRON_CLASSIFIER,  # Shorthand
    'mlp': MULTILAYER_CLASSIFIER,
}


CLASSIFICATION_RETURN_SERIES = ('classification.return_type', 'series')
CONFIDENCE_THRESHOLD = 0.5
FEATURE_MISSING_VALUE = 0.0

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
