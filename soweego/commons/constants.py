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
            QID: vocabulary.MUSICIAN_QID,
            MAIN_ENTITY: models.discogs_entity.DiscogsMusicianEntity,
            LINK_ENTITY: models.discogs_entity.DiscogsMusicianLinkEntity,
            NLP_ENTITY: models.discogs_entity.DiscogsMusicianNlpEntity,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        },
        BAND: {
            QID: vocabulary.BAND_QID,
            MAIN_ENTITY: models.discogs_entity.DiscogsGroupEntity,
            LINK_ENTITY: models.discogs_entity.DiscogsGroupLinkEntity,
            NLP_ENTITY: models.discogs_entity.DiscogsGroupNlpEntity,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        }
    },
    IMDB: {
        ACTOR: {
            QID: vocabulary.ACTOR_QID,
            MAIN_ENTITY: models.imdb_entity.ImdbActorEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        DIRECTOR: {
            QID: vocabulary.FILM_DIRECTOR_QID,
            MAIN_ENTITY: models.imdb_entity.ImdbDirectorEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        MUSICIAN: {
            QID: vocabulary.MUSICIAN_QID,
            MAIN_ENTITY: models.imdb_entity.ImdbMusicianEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        PRODUCER: {
            QID: vocabulary.FILM_PRODUCER_QID,
            MAIN_ENTITY: models.imdb_entity.ImdbProducerEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        },
        WRITER: {
            QID: vocabulary.SCREENWRITER_QID,
            MAIN_ENTITY: models.imdb_entity.ImdbWriterEntity,
            LINK_ENTITY: None,
            NLP_ENTITY: None,
            RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship
        }
    },
    MUSICBRAINZ: {
        MUSICIAN: {
            QID: vocabulary.MUSICIAN_QID,
            MAIN_ENTITY: models.musicbrainz_entity.MusicbrainzArtistEntity,
            LINK_ENTITY: models.musicbrainz_entity.MusicbrainzArtistLinkEntity,
            NLP_ENTITY: None,
            # TODO implement
            RELATIONSHIP_ENTITY: None
        },
        BAND: {
            QID: vocabulary.BAND_QID,
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

# File names & folders
SHARED_FOLDER = '/app/shared/'
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
WORKS_BY_PEOPLE_STATEMENTS = '%s_%s_works_by_people_statements.csv'

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
