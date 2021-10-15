#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import os
from pathlib import Path
from typing import TypeVar

from soweego.commons import keys
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity
from soweego.importer.models.discogs_entity import (
    DiscogsGroupEntity, DiscogsGroupLinkEntity, DiscogsGroupNlpEntity,
    DiscogsMasterArtistRelationship, DiscogsMasterEntity, DiscogsMusicianEntity,
    DiscogsMusicianLinkEntity, DiscogsMusicianNlpEntity
)
from soweego.importer.models.imdb_entity import (
    IMDbActorEntity, IMDbDirectorEntity, IMDbMusicianEntity, IMDbProducerEntity,
    IMDbTitleEntity, IMDbTitleNameRelationship, IMDbWriterEntity
)
from soweego.importer.models.musicbrainz_entity import (
    MusicBrainzArtistEntity, MusicBrainzArtistLinkEntity, MusicBrainzBandEntity,
    MusicBrainzBandLinkEntity, MusicBrainzReleaseGroupArtistRelationship,
    MusicBrainzReleaseGroupEntity, MusicBrainzReleaseGroupLinkEntity
)
from soweego.wikidata import vocabulary

##########
# Wikidata
##########
# Comply with https://meta.wikimedia.org/wiki/User-Agent_policy
HTTP_USER_AGENT = (
    'soweego/2.0 ([[:m:Grants:Project/Hjfocs/soweego_2]]; [[:m:User:Hjfocs]])'
)

# Wikidata items & properties regexes
QID_REGEX = r'Q\d+'
PID_REGEX = r'P\d+'

#############
# Directories
#############
# The root is 3 parents up from the current file 'soweego/commons/constants.py'
# NOTE: it must be changed if this file is moved
ROOT_DIR = Path(__file__).parent.parent.parent
WORK_DIR = 'work'
WD_DIR = 'wikidata'
SAMPLES_DIR = 'samples'
FEATURES_DIR = 'features'
MODELS_DIR = 'models'
RESULTS_DIR = 'results'
NN_CHECKPOINT_DIR = 'best_model_checkpoint'

############
# File names
############
CREDENTIALS_FILENAME = 'credentials.json'
NN_CHECKPOINT_FILENAME = '{}_best_checkpoint_model.hdf5'
EVALUATION_PERFORMANCE_FILENAME = '{}_{}_{}_performance.txt'
EVALUATION_PREDICTIONS_FILENAME = '{}_{}_{}_evaluation_links.csv.gz'
RESULT_FILENAME = '{}_{}_{}_links.csv.gz'
NESTED_CV_BEST_MODEL_FILENAME = '{}_{}_{}_best_model_k{:02}.pkl'
MODEL_FILENAME = '{}_{}_{}_model.pkl'
FEATURES_FILENAME = '{}_{}_{}_features{:02}.pkl.gz'
SAMPLES_FILENAME = '{}_{}_{}_samples{:02}.pkl.gz'
WD_CLASSIFICATION_SET_FILENAME = 'wikidata_{}_{}_classification_set.jsonl.gz'
WD_TRAINING_SET_FILENAME = 'wikidata_{}_{}_training_set.jsonl.gz'
EXTRACTED_LINKS_FILENAME = '{}_{}_extracted_links.csv'
BASELINE_PERFECT_FILENAME = '{}_{}_baseline_perfect_names.csv'
BASELINE_LINKS_FILENAME = '{}_{}_baseline_similar_links.csv'
BASELINE_NAMES_FILENAME = '{}_{}_baseline_similar_names.csv'
WIKIDATA_API_SESSION = 'wd_api_session.pkl'
WORKS_BY_PEOPLE_STATEMENTS = '%s_works_by_%s_statements.csv'

#######
# Paths
#######
# Default credentials are included in 'soweego/importer/resources':
# they should be loaded via `pkgutil.get_data(*DEFAULT_CREDENTIALS)`
DEFAULT_CREDENTIALS_MODULE = 'soweego.importer.resources'
DEFAULT_CREDENTIALS = (DEFAULT_CREDENTIALS_MODULE, CREDENTIALS_FILENAME)
# The user must put their 'credentials.json' file in the project root directory
USER_CREDENTIALS = os.path.join(ROOT_DIR, CREDENTIALS_FILENAME)

WD_TRAINING_SET = os.path.join(WD_DIR, WD_TRAINING_SET_FILENAME)
WD_CLASSIFICATION_SET = os.path.join(WD_DIR, WD_CLASSIFICATION_SET_FILENAME)
SAMPLES = os.path.join(SAMPLES_DIR, SAMPLES_FILENAME)
FEATURES = os.path.join(FEATURES_DIR, FEATURES_FILENAME)
LINKER_MODEL = os.path.join(MODELS_DIR, MODEL_FILENAME)
LINKER_NESTED_CV_BEST_MODEL = os.path.join(MODELS_DIR, NESTED_CV_BEST_MODEL_FILENAME)
LINKER_RESULT = os.path.join(RESULTS_DIR, RESULT_FILENAME)
LINKER_EVALUATION_PREDICTIONS = os.path.join(
    RESULTS_DIR, EVALUATION_PREDICTIONS_FILENAME
)
LINKER_PERFORMANCE = os.path.join(RESULTS_DIR, EVALUATION_PERFORMANCE_FILENAME)
NEURAL_NETWORK_CHECKPOINT_MODEL = os.path.join(
    NN_CHECKPOINT_DIR, NN_CHECKPOINT_FILENAME
)
EXTRACTED_LINKS = os.path.join(RESULTS_DIR, EXTRACTED_LINKS_FILENAME)
BASELINE_PERFECT = os.path.join(RESULTS_DIR, BASELINE_PERFECT_FILENAME)
BASELINE_LINKS = os.path.join(RESULTS_DIR, BASELINE_LINKS_FILENAME)
BASELINE_NAMES = os.path.join(RESULTS_DIR, BASELINE_NAMES_FILENAME)

#############################
# Catalogs & entities support
#############################
SUPPORTED_QUERY_TYPES = (keys.CLASS_QUERY, keys.OCCUPATION_QUERY)
SUPPORTED_QUERY_SELECTORS = (
    keys.IDENTIFIER,
    keys.LINKS,
    keys.DATASET,
    keys.BIODATA,
)

# Entities and corresponding Wikidata query
SUPPORTED_ENTITIES = {
    keys.ACTOR: keys.OCCUPATION_QUERY,
    keys.BAND: keys.CLASS_QUERY,
    keys.DIRECTOR: keys.OCCUPATION_QUERY,
    keys.MUSICIAN: keys.OCCUPATION_QUERY,
    keys.PRODUCER: keys.OCCUPATION_QUERY,
    keys.WRITER: keys.OCCUPATION_QUERY,
    keys.MUSICAL_WORK: keys.CLASS_QUERY,
    keys.AUDIOVISUAL_WORK: keys.CLASS_QUERY,
}

# Make Python types for DB entity objects: useful for type hints
DB_ENTITY = TypeVar('DB_ENTITY', BaseEntity, BaseLinkEntity, BaseNlpEntity)

# DB entities and their Wikidata class QID
TARGET_CATALOGS = {
    keys.DISCOGS: {
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: DiscogsMusicianEntity,
            keys.LINK_ENTITY: DiscogsMusicianLinkEntity,
            keys.NLP_ENTITY: DiscogsMusicianNlpEntity,
            keys.RELATIONSHIP_ENTITY: DiscogsMasterArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.BAND: {
            keys.CLASS_QID: vocabulary.BAND_QID,
            keys.MAIN_ENTITY: DiscogsGroupEntity,
            keys.LINK_ENTITY: DiscogsGroupLinkEntity,
            keys.NLP_ENTITY: DiscogsGroupNlpEntity,
            keys.RELATIONSHIP_ENTITY: DiscogsMasterArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.MUSICAL_WORK: {
            keys.CLASS_QID: vocabulary.MUSICAL_WORK_QID,
            keys.MAIN_ENTITY: DiscogsMasterEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: None,
        },
    },
    keys.IMDB: {
        keys.ACTOR: {
            keys.CLASS_QID: vocabulary.ACTOR_QID,
            keys.MAIN_ENTITY: IMDbActorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.DIRECTOR: {
            keys.CLASS_QID: vocabulary.FILM_DIRECTOR_QID,
            keys.MAIN_ENTITY: IMDbDirectorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: IMDbMusicianEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.PRODUCER: {
            keys.CLASS_QID: vocabulary.FILM_PRODUCER_QID,
            keys.MAIN_ENTITY: IMDbProducerEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.WRITER: {
            keys.CLASS_QID: vocabulary.SCREENWRITER_QID,
            keys.MAIN_ENTITY: IMDbWriterEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.AUDIOVISUAL_WORK: {
            keys.CLASS_QID: vocabulary.AUDIOVISUAL_WORK_QID,
            keys.MAIN_ENTITY: IMDbTitleEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: IMDbTitleNameRelationship,
            keys.WORK_TYPE: None,
        },
    },
    keys.MUSICBRAINZ: {
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: MusicBrainzArtistEntity,
            keys.LINK_ENTITY: MusicBrainzArtistLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.BAND: {
            keys.CLASS_QID: vocabulary.BAND_QID,
            keys.MAIN_ENTITY: MusicBrainzBandEntity,
            keys.LINK_ENTITY: MusicBrainzBandLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.MUSICAL_WORK: {
            keys.CLASS_QID: vocabulary.MUSICAL_WORK_QID,
            keys.MAIN_ENTITY: MusicBrainzReleaseGroupEntity,
            keys.LINK_ENTITY: MusicBrainzReleaseGroupLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: None,
        },
    },
}

# IMDb-specific
REQUIRE_OCCUPATION = {
    keys.IMDB: (
        keys.ACTOR,
        keys.DIRECTOR,
        keys.MUSICIAN,
        keys.PRODUCER,
        keys.WRITER,
    )
}

# Entity-specific
REQUIRE_GENRE = (keys.AUDIOVISUAL_WORK, keys.MUSICAL_WORK)
REQUIRE_PUBLICATION_DATE = (keys.AUDIOVISUAL_WORK, keys.MUSICAL_WORK)

# Fields holding name values
NAME_FIELDS = (
    keys.NAME,
    keys.ALIAS,
    keys.BIRTH_NAME,
    keys.FAMILY_NAME,
    keys.GIVEN_NAME,
    keys.PSEUDONYM,
    keys.REAL_NAME,
)

##################
# Machine learning
##################
CLASSIFIERS = {
    'naive_bayes': keys.NAIVE_BAYES,
    'logistic_regression': keys.LOGISTIC_REGRESSION,
    'support_vector_machines': keys.SVM,
    'linear_support_vector_machines': keys.LINEAR_SVM,
    'random_forest': keys.RANDOM_FOREST,
    'single_layer_perceptron': keys.SINGLE_LAYER_PERCEPTRON,
    'multi_layer_perceptron': keys.MULTI_LAYER_PERCEPTRON,
    'voting_classifier': keys.VOTING_CLASSIFIER,
    'gated_classifier': keys.GATED_CLASSIFIER,
    'stacked_classifier': keys.STACKED_CLASSIFIER,
    'nb': keys.NAIVE_BAYES,  # Shorthand
    'lr': keys.LOGISTIC_REGRESSION,  # Shorthand
    'svm': keys.SVM,  # Shorthand
    'lsvm': keys.LINEAR_SVM,  # Shorthand
    'rf': keys.RANDOM_FOREST,  # Shorthand
    'slp': keys.SINGLE_LAYER_PERCEPTRON,  # Shorthand
    'mlp': keys.MULTI_LAYER_PERCEPTRON,  # Shorthand
    'vc': keys.VOTING_CLASSIFIER,  # Shorthand
    'gc': keys.GATED_CLASSIFIER,  # Shorthand
    'sc': keys.STACKED_CLASSIFIER,  # Shorthand
}

# Holds mention of 'classifier ensemble'
CLASSIFIERS_FOR_ENSEMBLE = [
    keys.NAIVE_BAYES,
    keys.LOGISTIC_REGRESSION,
    keys.RANDOM_FOREST,
    keys.SINGLE_LAYER_PERCEPTRON,
    keys.MULTI_LAYER_PERCEPTRON,
]

PERFORMANCE_METRICS = ['precision', 'recall', 'f1']

# Grid search settings
PARAMETER_GRIDS = {
    keys.NAIVE_BAYES: {
        'alpha': [0.0001, 0.001, 0.01, 0.1, 1],
        'binarize': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
    },
    keys.LOGISTIC_REGRESSION: {
        'tol': [1e-3, 1e-4, 1e-5],
        'C': [0.01, 0.1, 1.0, 10, 100],
        'class_weight': [None, 'balanced'],
        'solver': ['liblinear', 'lbfgs', 'saga', 'sag'],
        'max_iter': [100, 200],
    },
    keys.LINEAR_SVM: {
        'dual': [True, False],
        'tol': [1e-3, 1e-4, 1e-5],
        'max_iter': [1000, 2000],
        # liblinear fails to converge when C values are 10 and 100 in some datasets
        'C': [0.01, 0.1, 1.0, 10, 100],
    },
    keys.SVM: {
        # The execution takes too long when C=100 and kernel=linear
        'C': [0.01, 0.1, 1.0, 10],
        'kernel': ['linear', 'poly', 'rbf', 'sigmoid'],
        'gamma': ['auto', 'scale'],
        'tol': [1e-3, 1e-4],
    },
    keys.RANDOM_FOREST: {
        'n_estimators': [100, 200, 350, 500],
        'criterion': ['gini', 'entropy'],
        'max_features': ['sqrt', 'log2', None],
        'bootstrap': [True, False],
    },
    keys.SINGLE_LAYER_PERCEPTRON: {
        'epochs': [100, 1000, 2000, 3000],
        'batch_size': [256, 512, 1024, 2048],
        'activation': ['sigmoid'],
        'optimizer': ['adam', 'RMSprop', 'Adadelta', 'Nadam'],
    },
    keys.MULTI_LAYER_PERCEPTRON: {
        'epochs': [1000, 2000],
        'batch_size': [512],
        'hidden_activation': ['relu', 'tanh', 'selu'],
        'output_activation': ['sigmoid'],
        'optimizer': ['adam', 'Adadelta', 'Nadam'],
        'hidden_layer_dims': [[128, 32], [256, 128, 32], [128, 32, 32]],
    },
}

CLASSIFICATION_RETURN_SERIES = ('classification.return_type', 'series')
CLASSIFICATION_RETURN_INDEX = ('classification.return_type', 'index')
CONFIDENCE_THRESHOLD = 0.5
FEATURE_MISSING_VALUE = 0.0

# Neural Networks general settings
LOSS = 'binary_crossentropy'
METRICS = ['accuracy']
VALIDATION_SPLIT = 0.33

# Per-model hyperparameters
NAIVE_BAYES_PARAMS = {'alpha': 0.0001, 'binarize': 0.2}
LOGISTIC_REGRESSION_PARAMS = {
    'tol': 0.001,
    'C': 1.0,
    'class_weight': None,
    'solver': 'liblinear',
    'max_iter': 100,
}
LINEAR_SVM_PARAMS = {'dual': True, 'tol': 0.001, 'max_iter': 1000, 'C': 1.0}
RANDOM_FOREST_PARAMS = {
    'n_estimators': 500,
    'criterion': 'entropy',
    'max_features': None,
    'bootstrap': True,
}
SINGLE_LAYER_PERCEPTRON_PARAMS = {
    'epochs': 1000,
    'batch_size': 256,
    'activation': 'sigmoid',
    'optimizer': 'Nadam',
}
MULTI_LAYER_PERCEPTRON_PARAMS = {
    'epochs': 1000,
    'batch_size': 512,
    'hidden_activation': 'selu',
    'output_activation': 'sigmoid',
    'optimizer': 'Adadelta',
    'hidden_layer_dims': (
        # specifies a two fully connected layer NN
        # an extra layer with 1 output dimension will be
        # automatically used
        128,
        32,
    ),
}

# Ensemble hyperparameters
VOTING_CLASSIFIER_PARAMS = {'voting': 'soft'}
GATED_ENSEMBLE_PARAMS = {'folds': 2, 'meta_layer': keys.SINGLE_LAYER_PERCEPTRON}
STACKED_ENSEMBLE_PARAMS = {
    'folds': 2,
    'meta_layer': keys.SINGLE_LAYER_PERCEPTRON,
}

# Precisions for the `pandas.Period` class.
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
