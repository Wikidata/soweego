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
import recordlinkage as rl
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
        dir_io, constants.LINKER_MODEL % (target, classifier))
    joblib.dump(model, outfile)
    LOGGER.info("%s model dumped to '%s'", classifier, outfile)


def execute(classifier, catalog, entity, binarize, dir_io):
    wd_reader, target_reader = workflow.train_test_build(
        catalog, entity, dir_io)
    wd, target = workflow.preprocess(
        'training', catalog, wd_reader, target_reader, dir_io)
    positive_samples = blocking.train_test_block(wd, target)
    samples = blocking.full_text_query_block(
        'training', catalog, wd, target_database.get_entity(catalog, entity), dir_io)
    actual_positive = samples & positive_samples
    positive_size, actual_size = len(positive_samples), len(actual_positive)
    if positive_size != actual_size:
        LOGGER.warning(
            '%d positive samples are not included in the full set of samples and will not be used', positive_size - actual_size)
    feature_vectors = workflow.extract_features(samples, wd, target)
    return _train(classifier, feature_vectors, actual_positive, binarize)


def _train(classifier, feature_vectors, positive_samples_index, binarize):
    model = workflow.init_model(classifier, binarize)
    LOGGER.info('Training a %s', classifier.__name__)
    model.fit(feature_vectors, positive_samples_index)
    LOGGER.info('Training done')
    return model


if __name__ == "__main__":
    m = execute(rl.NaiveBayesClassifier, 'discogs',
                'musician', 0.3, '/Users/focs/soweego/output')
