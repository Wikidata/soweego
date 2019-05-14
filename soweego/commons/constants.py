#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import TypeVar

from soweego.commons import keys
from soweego.importer import models
from soweego.wikidata import vocabulary

SUPPORTED_QUERY_TYPES = (keys.CLASS, keys.OCCUPATION)
SUPPORTED_QUERY_SELECTORS = (
    keys.IDENTIFIER, keys.LINKS, keys.DATASET, keys.METADATA)

# Entities and corresponding Wikidata query
SUPPORTED_ENTITIES = {
    keys.ACTOR: keys.OCCUPATION,
    keys.BAND: keys.CLASS,
    keys.DIRECTOR: keys.OCCUPATION,
    keys.MUSICIAN: keys.OCCUPATION,
    keys.PRODUCER: keys.OCCUPATION,
    keys.WRITER: keys.OCCUPATION,
    keys.MUSICAL_WORK: keys.CLASS,
    keys.AUDIOVISUAL_WORK: keys.CLASS
}

# Target catalogs imported into the internal DB
# DB entity Python types for typed function signatures
DB_ENTITY = TypeVar('DB_ENTITY', models.base_entity.BaseEntity,
                    models.base_link_entity.BaseLinkEntity, models.base_nlp_entity.BaseNlpEntity)

# DB entities and their Wikidata class QID
# TODO merge discogs & musicbrainz 'release' entities with issue-80 branch
TARGET_CATALOGS = {
    keys.DISCOGS: {
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: models.discogs_entity.DiscogsMusicianEntity,
            keys.LINK_ENTITY: models.discogs_entity.DiscogsMusicianLinkEntity,
            keys.NLP_ENTITY: models.discogs_entity.DiscogsMusicianNlpEntity,
            # TODO implement
            keys.RELATIONSHIP_ENTITY: None
        },
        keys.BAND: {
            keys.CLASS_QID: vocabulary.BAND_QID,
            keys.MAIN_ENTITY: models.discogs_entity.DiscogsGroupEntity,
            keys.LINK_ENTITY: models.discogs_entity.DiscogsGroupLinkEntity,
            keys.NLP_ENTITY: models.discogs_entity.DiscogsGroupNlpEntity,
            # TODO implement
            keys.RELATIONSHIP_ENTITY: None
        }
    },
    keys.IMDB: {
        keys.ACTOR: {
            keys.CLASS_QID: vocabulary.ACTOR_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbActorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK
        },
        keys.DIRECTOR: {
            keys.CLASS_QID: vocabulary.FILM_DIRECTOR_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbDirectorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK
        },
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbMusicianEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK
        },
        keys.PRODUCER: {
            keys.CLASS_QID: vocabulary.FILM_PRODUCER_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbProducerEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK
        },
        keys.WRITER: {
            keys.CLASS_QID: vocabulary.SCREENWRITER_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbWriterEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK
        },
        keys.AUDIOVISUAL_WORK: {
            keys.CLASS_QID: vocabulary.AUDIOVISUAL_WORK_QID,
            keys.MAIN_ENTITY: models.imdb_entity.ImdbMovieEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: models.imdb_entity.ImdbPersonMovieRelationship,
            keys.WORK_TYPE: None
        }
    },
    keys.MUSICBRAINZ: {
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: models.musicbrainz_entity.MusicbrainzArtistEntity,
            keys.LINK_ENTITY: models.musicbrainz_entity.MusicbrainzArtistLinkEntity,
            keys.NLP_ENTITY: None,
            # TODO implement
            keys.RELATIONSHIP_ENTITY: None
        },
        keys.BAND: {
            keys.CLASS_QID: vocabulary.BAND_QID,
            keys.MAIN_ENTITY: models.musicbrainz_entity.MusicbrainzBandEntity,
            keys.LINK_ENTITY: models.musicbrainz_entity.MusicbrainzBandLinkEntity,
            keys.NLP_ENTITY: None,
            # TODO implement
            keys.RELATIONSHIP_ENTITY: None
        }
    }
}

# When building the wikidata dump for catalogs in this array
# also the QIDs of a person's occupations will be included
# as part of the dump
REQUIRE_OCCUPATIONS = [
    keys.IMDB
]

# Cluster of fields with names
NAME_FIELDS = (keys.NAME, keys.ALIAS, keys.BIRTH_NAME,
               keys.FAMILY_NAME, keys.GIVEN_NAME, keys.PSEUDONYM, keys.REAL_NAME)

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

CLASSIFIERS = {
    'naive_bayes': keys.NAIVE_BAYES,
    'support_vector_machines': keys.SVM,
    'linear_support_vector_machines': keys.LINEAR_SVM,
    'single_layer_perceptron': keys.SINGLE_LAYER_PERCEPTRON,
    'multi_layer_perceptron': keys.MULTI_LAYER_PERCEPTRON,
    'nb': keys.NAIVE_BAYES,  # Shorthand
    'svm': keys.SVM,  # Shorthand
    'lsvm': keys.LINEAR_SVM,  # Shorthand
    'slp': keys.SINGLE_LAYER_PERCEPTRON,  # Shorthand
    'mlp': keys.MULTI_LAYER_PERCEPTRON  # Shorthand
}

PERFORMANCE_METRICS = ['precision', 'recall', 'f1']

PARAMETER_GRIDS = {
    keys.NAIVE_BAYES: {
        'alpha': [0.0001, 0.001, 0.01, 0.1, 1],
        'binarize': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    },
    keys.LINEAR_SVM: {
        # liblinear fails to converge when values are 10 and 100 in some datasets
        'C': [0.01, 0.1, 1.0, 10, 100]
    },
    keys.SVM: {
        # The execution takes too long when C=100 and kernel=linear
        'C': [0.01, 0.1, 1.0, 10],
        'kernel': ['linear', 'poly', 'rbf', 'sigmoid']
    },
    keys.SINGLE_LAYER_PERCEPTRON: {
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
