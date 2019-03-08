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
from itertools import tee

import click
import recordlinkage as rl
from pandas import MultiIndex, concat
from sklearn.externals import joblib

from soweego.commons import constants, target_database
from soweego.linker import blocking, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-b', '--binarize', default=0.1, help="Default: 0.1")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default='/app/shared', help="Input/output directory, default: '/app/shared'.")
def cli(classifier, target, target_type, binarize, dir_io):
    """Train a probabilistic linker."""

    model = execute(
        constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)
    outfile = os.path.join(
        dir_io, constants.LINKER_MODEL % (target, target_type, classifier))
    joblib.dump(model, outfile)
    LOGGER.info("%s model dumped to '%s'", classifier, outfile)


def execute(classifier, catalog, entity, binarize, dir_io):
    wd_reader, target_reader = workflow.train_test_build(
        catalog, entity, dir_io)

    wd_reader1, wd_reader2 = tee(wd_reader)
    positive_samples_index = _build_positive_samples_index(wd_reader1)

    wd_generator, target_generator = workflow.preprocess(
        'training', wd_reader2, target_reader)

    feature_vectors = []
    for i, wd_chunk in enumerate(wd_generator, 1):
        for target_chunk in target_generator:
            all_samples = blocking.full_text_query_block(
                'training', catalog, wd_chunk, i, target_database.get_entity(catalog, entity), dir_io)
            feature_vectors.append(
                workflow.extract_features(all_samples, wd_chunk, target_chunk))
    return _train(classifier, concat(feature_vectors), positive_samples_index, binarize)


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
    model = workflow.init_model(classifier, binarize)
    LOGGER.info('Training a %s', classifier.__name__)
    model.fit(feature_vectors, positive_samples_index)
    LOGGER.info('Training done')
    return model
