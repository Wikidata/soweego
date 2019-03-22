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
from pandas import concat
from sklearn.model_selection import KFold, train_test_split

from soweego.commons import constants, target_database
from soweego.linker import train, workflow

LOGGER = logging.getLogger(__name__)
rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-b', '--binarize', default=0.1, help="Default: 0.1")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def cli(classifier, target, target_type, binarize, dir_io):
    """Evaluate the performance of a probabilistic linker."""
    precision, recall, fscore, confusion_matrix = k_fold_single_evaluation(
        constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)
    with open(os.path.join(dir_io, constants.LINKER_PERFORMANCE % (target, target_type, classifier)), 'w') as fileout:
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


def k_fold_single_evaluation(classifier, catalog, entity, binarize, dir_io, k=5):
    predictions, test_set = [], []
    dataset, positive_samples_index = train.build_dataset(
        'training', catalog, entity, dir_io)
    k_fold = KFold(n_splits=k, shuffle=True)

    for train_index, test_index in k_fold.split(dataset):
        training, test = dataset.iloc[train_index], dataset.iloc[test_index]
        test_set.append(test)
        model = workflow.init_model(classifier, binarize)
        model.fit(training, positive_samples_index & training.index)
        predictions.append(model.predict(test))

    test_set = concat(test_set)
    return _compute_performance(positive_samples_index & test_set.index, concat(predictions), len(test_set))


def _random_split(wd_chunk, target_chunk):
    wd_train, wd_test = train_test_split(wd_chunk, test_size=0.33)
    target_train, target_test = train_test_split(
        target_chunk, test_size=0.33)
    return wd_train, target_train, wd_test, target_test
