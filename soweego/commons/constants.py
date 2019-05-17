#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import TypeVar

from soweego.importer import models, discogs_dump_extractor, musicbrainz_dump_extractor, imdb_dump_extractor
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
OCCUPATION = 'occupation'
SUPPORTED_QUERY_TYPES = (CLASS, OCCUPATION)
SUPPORTED_QUERY_SELECTORS = (IDENTIFIER, LINKS, DATASET, METADATA)

# Entity types and corresponding Wikidata query
HANDLED_ENTITIES = {
    'band': CLASS,
    'actor': OCCUPATION,
    'director': OCCUPATION,
    'musician': OCCUPATION,
    'producer': OCCUPATION,
    'writer': OCCUPATION,
    'release': CLASS,
    'movie': CLASS
}

# DB entity Python types for typed function signatures
DB_ENTITY = TypeVar('DB_ENTITY', models.base_entity.BaseEntity,
                    models.base_link_entity.BaseLinkEntity, models.base_nlp_entity.BaseNlpEntity)

# Dump extractors
DUMP_EXTRACTOR = {
    'discogs': discogs_dump_extractor.DiscogsDumpExtractor,
    'musicbrainz': musicbrainz_dump_extractor.MusicBrainzDumpExtractor,
    'imdb': imdb_dump_extractor.ImdbDumpExtractor
}

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
        },
        'release': {
            'qid': vocabulary.MUSICALWORK,
            'entity': models.discogs_entity.DiscogsMasterEntity,
            'link_entity': None,
            'nlp_entity': None
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
        },
        'movie': {
            'qid': vocabulary.AUDIOVISUAL_WORK,
            'entity': models.imdb_entity.ImdbMovieEntity,
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
        },
        'release': {
            'qid': vocabulary.MUSICALWORK,
            'entity': models.musicbrainz_entity.MusicbrainzReleaseGroupEntity,
            'link_entity': models.musicbrainz_entity.MusicbrainzReleaseGroupLinkEntity,
            'nlp_entity': None
        }
    }
}

# When building the wikidata dump for catalogs in this array
# also the QIDs of a person's occupations will be included
# as part of the dump
REQUIRE_OCCUPATIONS = {
    'imdb': ['actor', 'director', 'musician', 'producer', 'write']
}

REQUIRE_GENRE = [
    'release', 'movie'
]

# Wikidata field & target column names
INTERNAL_ID = 'internal_id'
CATALOG_ID = 'catalog_id'
QID = 'qid'
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
GENRE = 'genres'
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
LINKER_NESTED_CV_BEST_MODEL = '%s_%s_%s_best_model_k%02d.pkl'
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
NAIVE_BAYES = 'naive_bayes'
LINEAR_SVM = 'linear_support_vector_machines'
SVM = 'support_vector_machines'
SINGLE_LAYER_PERCEPTRON = 'single_layer_perceptron'
MULTILAYER_CLASSIFIER = 'multi_layer_perceptron'

CLASSIFIERS = {
    'naive_bayes': NAIVE_BAYES,
    'support_vector_machines': SVM,
    'linear_support_vector_machines': LINEAR_SVM,
    'single_layer_perceptron': SINGLE_LAYER_PERCEPTRON,
    'multi_layer_perceptron': MULTILAYER_CLASSIFIER,
    'nb': NAIVE_BAYES,  # Shorthand
    'svm': SVM,  # Shorthand
    'lsvm': LINEAR_SVM,  # Shorthand
    'slp': SINGLE_LAYER_PERCEPTRON,  # Shorthand
    'mlp': MULTILAYER_CLASSIFIER  # Shorthand
}

PERFORMANCE_METRICS = ['precision', 'recall', 'f1']

PARAMETER_GRIDS = {
    NAIVE_BAYES: {
        'alpha': [0.0001, 0.001, 0.01, 0.1, 1],
        'binarize': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    },
    LINEAR_SVM: {
        # liblinear fails to converge when values are 10 and 100 in some datasets
        'C': [0.01, 0.1, 1.0, 10, 100]
    },
    SVM: {
        # The execution takes too long when C=100 and kernel=linear
        'C': [0.01, 0.1, 1.0, 10],
        'kernel': ['linear', 'poly', 'rbf', 'sigmoid']
    },
    SINGLE_LAYER_PERCEPTRON: {
        'epochs': [100, 1000, 2000, 3000],
        'batch_size': [256, 512, 1024, 2048]
    }
}

CLASSIFICATION_RETURN_SERIES = ('classification.return_type', 'series')
CONFIDENCE_THRESHOLD = 0.5
FEATURE_MISSING_VALUE = 0.0

# Neural networks-specific
ACTIVATION = 'sigmoid'
OPTIMIZER = 'adam'
LOSS = 'binary_crossentropy'
METRICS = ['accuracy']
BATCH_SIZE = 1024
EPOCHS = 1000
VALIDATION_SPLIT = 0.33

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
