#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Evaluation of supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click
import recordlinkage as rl
from sklearn.model_selection import train_test_split

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
    """Evaluate the performance of a probabilistic linker."""
    result = evaluate(
        constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)

    for predictions, (precision, recall, fscore, confusion_matrix) in result:
        predictions.to_series().to_csv(os.path.join(dir_io, constants.LINKER_EVALUATION_PREDICTIONS %
                                                    (target, classifier)), mode='a', columns=[], header=True)
        with open(os.path.join(dir_io, constants.LINKER_PERFORMANCE % (target, classifier)), 'a') as fileout:
            fileout.write(
                f'Precision: {precision}\nRecall: {recall}\nF-score: {fscore}\nConfusion matrix:\n{confusion_matrix}\n')


def _compute_performance(test_index, predictions, test_vectors_size):

    LOGGER.info('Running performance evaluation ...')

    confusion_matrix = rl.confusion_matrix(
        test_index, predictions, total=test_vectors_size)
    precision = rl.precision(test_index, predictions)
    recall = rl.recall(test_index, predictions)
    fscore = rl.fscore(confusion_matrix)

    LOGGER.info('Precision: %f - Recall: %f - F-score: %f',
                precision, recall, fscore)
    LOGGER.info('Confusion matrix: %s', confusion_matrix)

    return precision, recall, fscore, confusion_matrix


def evaluate(classifier, catalog, entity, binarize, dir_io):
    result = []

    # Build
    wd_reader, target_reader = workflow.train_test_build(
        catalog, entity, dir_io)
    wd_generator, target_generator = workflow.preprocess(
        'training', wd_reader, target_reader)

    for i, wd_chunk in enumerate(wd_generator, 1):
        for target_chunk in target_generator:
            # 1. Random split (2/3 train, 1/3 test)
            wd_train, target_train, wd_test, target_test = _random_split(
                wd_chunk, target_chunk)

            # 2. Train
            train_vectors, train_positives_index = _workflow(
                wd_train, target_train, i, catalog, entity, dir_io)

            # 3. Build model
            model = workflow.init_model(classifier, binarize)
            model.fit(train_vectors, train_positives_index)

            # 4. Test
            test_vectors, test_positive_index = _workflow(
                wd_test, target_test, i, catalog, entity, dir_io)
            predictions = model.predict(test_vectors)

            result.append((predictions, _compute_performance(
                test_positive_index, predictions, len(test_vectors))))

    return result


def _workflow(wikidata, target, wd_chunk_number, catalog, entity, dir_io):
    train_positives = blocking.train_test_block(wikidata, target)
    train_all = blocking.full_text_query_block(
        'training', catalog, wikidata, wd_chunk_number, target_database.get_entity(catalog, entity), dir_io)
    train_actual = train_all & train_positives
    train_vectors = workflow.extract_features(
        train_all, wikidata, target)
    return train_vectors, train_actual


def _random_split(wd_chunk, target_chunk):
    wd_train, wd_test = train_test_split(wd_chunk, test_size=0.33)
    target_train, target_test = train_test_split(
        target_chunk, test_size=0.33)
    return wd_train, target_train, wd_test, target_test
