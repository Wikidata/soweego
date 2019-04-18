#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Training set construction for supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click
from pandas import MultiIndex, concat
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, target_database
from soweego.linker import blocking, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-b', '--binarize', default=0.1, help="Default: 0.1")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def cli(classifier, target, target_type, binarize, dir_io):
    """Train a probabilistic linker."""

    model = execute(
        constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)
    outfile = os.path.join(
        dir_io, constants.LINKER_MODEL % (target, target_type, classifier))
    joblib.dump(model, outfile)
    LOGGER.info("%s model dumped to '%s'", classifier, outfile)


def execute(classifier, catalog, entity, binarize, dir_io):
    goal = 'training'

    feature_vectors, positive_samples_index = build_dataset(
        goal, catalog, entity, dir_io)

    return _train(classifier, feature_vectors, positive_samples_index, binarize)


def build_dataset(goal, catalog, entity, dir_io):
    wd_reader = workflow.build_wikidata(goal, catalog, entity, dir_io)
    wd_generator = workflow.preprocess_wikidata(goal, wd_reader)

    positive_samples, feature_vectors = [], []

    for i, wd_chunk in enumerate(wd_generator, 1):
        # Positive samples from Wikidata
        positive_samples.append(wd_chunk[constants.TID])

        # Samples index from Wikidata
        all_samples = blocking.full_text_query_block(
            goal, catalog, wd_chunk[constants.NAME_TOKENS], i, target_database.get_entity(catalog, entity), dir_io)

        # Build target chunk based on samples
        target_reader = data_gathering.gather_target_dataset(
            goal, entity, catalog, set(all_samples.get_level_values(constants.TID)))

        # Preprocess target chunk
        target_chunk = workflow.preprocess_target(
            goal, target_reader)

        features_path = os.path.join(
            dir_io, constants.FEATURES % (catalog, entity, goal, i))

        feature_vectors.append(workflow.extract_features(
            all_samples, wd_chunk, target_chunk, features_path))

    positive_samples = concat(positive_samples)
    positive_samples_index = MultiIndex.from_tuples(zip(
        positive_samples.index, positive_samples), names=[constants.QID, constants.TID])

    LOGGER.info('Built positive samples index from Wikidata')
    return concat(feature_vectors, sort=False).fillna(constants.FEATURE_MISSING_VALUE), positive_samples_index


def _build_positive_samples_index(wd_reader1):
    LOGGER.info('Building positive samples index from Wikidata ...')
    positive_samples = []
    for chunk in wd_reader1:
        # TODO don't wipe out QIDs with > 1 positive samples!
        tids_series = chunk.set_index(constants.QID)[constants.TID].map(
            lambda cell: cell[0] if isinstance(cell, list) else cell)
        positive_samples.append(tids_series)

    positive_samples = concat(positive_samples)
    positive_samples_index = MultiIndex.from_tuples(zip(
        positive_samples.index, positive_samples), names=[constants.QID, constants.TID])
    LOGGER.info('Built positive samples index from Wikidata')
    return positive_samples_index


def _train(classifier, feature_vectors, positive_samples_index, binarize):
    
    model = workflow.init_model(classifier, binarize, feature_vectors.shape[1])
    
    LOGGER.info('Training a %s', classifier)
    model.fit(feature_vectors, positive_samples_index)

    LOGGER.info('Training done')
    return model
