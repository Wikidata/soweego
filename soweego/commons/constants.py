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
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.importer.models.base_nlp_entity import BaseNlpEntity
from soweego.importer.models.discogs_entity import (
    DiscogsGroupEntity,
    DiscogsGroupLinkEntity,
    DiscogsGroupNlpEntity,
    DiscogsMasterArtistRelationship,
    DiscogsMasterEntity,
    DiscogsMusicianEntity,
    DiscogsMusicianLinkEntity,
    DiscogsMusicianNlpEntity,
)
from soweego.importer.models.imdb_entity import (
    ImdbActorEntity,
    ImdbDirectorEntity,
    ImdbMovieEntity,
    ImdbMoviePersonRelationship,
    ImdbMusicianEntity,
    ImdbProducerEntity,
    ImdbWriterEntity,
)
from soweego.importer.models.musicbrainz_entity import (
    MusicbrainzArtistEntity,
    MusicbrainzArtistLinkEntity,
    MusicbrainzBandEntity,
    MusicbrainzBandLinkEntity,
    MusicBrainzReleaseGroupArtistRelationship,
    MusicbrainzReleaseGroupEntity,
    MusicbrainzReleaseGroupLinkEntity,
)
from soweego.wikidata import vocabulary

# As per https://meta.wikimedia.org/wiki/User-Agent_policy
HTTP_USER_AGENT = 'soweego/1.0 ([[:m:Grants:Project/Hjfocs/soweego]]; [[:m:User:Hjfocs]])'

# Wikidata items & properties regexes
QID_REGEX = r'Q\d+'
PID_REGEX = r'P\d+'

# Entities and corresponding Wikidata query
SUPPORTED_QUERY_TYPES = (keys.CLASS_QUERY, keys.OCCUPATION_QUERY)
SUPPORTED_QUERY_SELECTORS = (
    keys.IDENTIFIER,
    keys.LINKS,
    keys.DATASET,
    keys.METADATA,
)

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

# Target catalogs imported into the internal DB
# DB entity Python types for typed function signatures
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
            keys.MAIN_ENTITY: ImdbActorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.DIRECTOR: {
            keys.CLASS_QID: vocabulary.FILM_DIRECTOR_QID,
            keys.MAIN_ENTITY: ImdbDirectorEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: ImdbMusicianEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.PRODUCER: {
            keys.CLASS_QID: vocabulary.FILM_PRODUCER_QID,
            keys.MAIN_ENTITY: ImdbProducerEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.WRITER: {
            keys.CLASS_QID: vocabulary.SCREENWRITER_QID,
            keys.MAIN_ENTITY: ImdbWriterEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: keys.AUDIOVISUAL_WORK,
        },
        keys.AUDIOVISUAL_WORK: {
            keys.CLASS_QID: vocabulary.AUDIOVISUAL_WORK_QID,
            keys.MAIN_ENTITY: ImdbMovieEntity,
            keys.LINK_ENTITY: None,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: ImdbMoviePersonRelationship,
            keys.WORK_TYPE: None,
        },
    },
    keys.MUSICBRAINZ: {
        keys.MUSICIAN: {
            keys.CLASS_QID: vocabulary.MUSICIAN_QID,
            keys.MAIN_ENTITY: MusicbrainzArtistEntity,
            keys.LINK_ENTITY: MusicbrainzArtistLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.BAND: {
            keys.CLASS_QID: vocabulary.BAND_QID,
            keys.MAIN_ENTITY: MusicbrainzBandEntity,
            keys.LINK_ENTITY: MusicbrainzBandLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: keys.MUSICAL_WORK,
        },
        keys.MUSICAL_WORK: {
            keys.CLASS_QID: vocabulary.MUSICAL_WORK_QID,
            keys.MAIN_ENTITY: MusicbrainzReleaseGroupEntity,
            keys.LINK_ENTITY: MusicbrainzReleaseGroupLinkEntity,
            keys.NLP_ENTITY: None,
            keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
            keys.WORK_TYPE: None,
        },
    },
}

# When building the wikidata dump for catalogs in this array
# also the QIDs of a person's occupations will be included
# as part of the dump
REQUIRE_OCCUPATION = {
    keys.IMDB: (
        keys.ACTOR,
        keys.DIRECTOR,
        keys.MUSICIAN,
        keys.PRODUCER,
        keys.WRITER,
    )
}
REQUIRE_GENRE = (keys.AUDIOVISUAL_WORK, keys.MUSICAL_WORK)
REQUIRE_PUBLICATION_DATE = (keys.AUDIOVISUAL_WORK, keys.MUSICAL_WORK)

# Cluster of fields with names
NAME_FIELDS = (
    keys.NAME,
    keys.ALIAS,
    keys.BIRTH_NAME,
    keys.FAMILY_NAME,
    keys.GIVEN_NAME,
    keys.PSEUDONYM,
    keys.REAL_NAME,
)

# File names & folders
SHARED_FOLDER = '/app/shared/'
WD_TRAINING_SET = 'wikidata/wikidata_%s_%s_training_set.jsonl.gz'
WD_CLASSIFICATION_SET = 'wikidata/wikidata_%s_%s_classification_set.jsonl.gz'
SAMPLES = 'samples/%s_%s_%s_samples%02d.pkl.gz'
FEATURES = 'features/%s_%s_%s_features%02d.pkl.gz'
LINKER_MODEL = 'models/%s_%s_%s_model.pkl'
LINKER_NESTED_CV_BEST_MODEL = '%models/s_%s_%s_best_model_k%02d.pkl'
LINKER_RESULT = 'results/%s_%s_%s_linker_result.csv.gz'
LINKER_EVALUATION_PREDICTIONS = 'results/%s_%s_%s_linker_evaluation_predictions.csv.gz'
LINKER_PERFORMANCE = 'results/%s_%s_%s_linker_performance.txt'
NEURAL_NETWORK_CHECKPOINT_MODEL = 'best_model_checkpoint/%s_best_checkpoint_model.hdf5'
COMPLETE_FEATURE_VECTORS = 'features/%s_%s_%s_complete_feature_vectors.pkl.gz'
COMPLETE_WIKIDATA_CHUNKS = 'wikidata/%s_%s_%s_complete_wikidata_chunks.pkl.gz'
COMPLETE_TARGET_CHUNKS = 'samples/%s_%s_%s_complete_target_chunks.pkl.gz'
COMPLETE_POSITIVE_SAMPLES_INDEX = (
    'samples/%s_%s_%s_complete_positive_samples_index.pkl.gz'
)
WIKIDATA_API_SESSION = 'wiki_api_session.pkl'
WORKS_BY_PEOPLE_STATEMENTS = '%s_works_by_%s_statements.csv'
TENSOR_BOARD = 'tensor_board/'

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
    'mlp': keys.MULTI_LAYER_PERCEPTRON,  # Shorthand
}

PERFORMANCE_METRICS = ['precision', 'recall', 'f1']

PARAMETER_GRIDS = {
    keys.NAIVE_BAYES: {
        'alpha': [0.0001, 0.001, 0.01, 0.1, 1],
        'binarize': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
    },
    keys.LINEAR_SVM: {
        # liblinear fails to converge when values are 10 and 100 in some datasets
        'C': [0.01, 0.1, 1.0, 10, 100]
    },
    keys.SVM: {
        # The execution takes too long when C=100 and kernel=linear
        'C': [0.01, 0.1, 1.0, 10],
        'kernel': ['linear', 'poly', 'rbf', 'sigmoid'],
    },
    keys.SINGLE_LAYER_PERCEPTRON: {
        'epochs': [100, 1000, 2000, 3000],
        'batch_size': [256, 512, 1024, 2048],
    },
}

CLASSIFICATION_RETURN_SERIES = ('classification.return_type', 'series')
CLASSIFICATION_RETURN_INDEX = ('classification.return_type', 'index')
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
