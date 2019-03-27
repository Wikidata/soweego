#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Evaluation of supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging
import os

import click
import recordlinkage as rl
from numpy import mean, std
from pandas import concat
from sklearn.model_selection import StratifiedKFold, train_test_split

from soweego.commons import constants, target_database
from soweego.linker import train, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('--single', is_flag=True, help='Compute a single evaluation over all k folds, instead of k evaluations.')
@click.option('-b', '--binarize', default=0.1, help="Default: 0.1.")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER, help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def cli(classifier, target, target_type, binarize, single, dir_io):
    """Evaluate the performance of a probabilistic linker."""
    if not single:
        predictions, p_mean, p_std, r_mean, r_std, fscore_mean, fscore_std = average_k_fold(
            constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)
        LOGGER.info('Precision: mean = %s; std = %s; Recall: mean = %s; std = %s; F-score: mean = %s; std = %s',
                    p_mean, p_std, r_mean, r_std, fscore_mean, fscore_std)

        predictions.to_series().to_csv(os.path.join(dir_io, constants.LINKER_EVALUATION_PREDICTIONS %
                                                    (target, target_type, classifier)), columns=[], header=True)
        with open(os.path.join(dir_io, constants.LINKER_PERFORMANCE % (target, target_type, classifier)), 'w') as fileout:
            fileout.write(
                f'Precision:\n\tmean = {p_mean}\n\tstandard deviation = {p_std}\nRecall:\n\tmean = {r_mean}\n\tstandard deviation = {r_std}\nF-score:\n\tmean = {fscore_mean}\n\tstandard deviation = {fscore_std}\n')
    else:
        predictions, (precision, recall, fscore, confusion_matrix) = single_k_fold(
            constants.CLASSIFIERS[classifier], target, target_type, binarize, dir_io)

        predictions.to_series().to_csv(os.path.join(dir_io, constants.LINKER_EVALUATION_PREDICTIONS %
                                                    (target, target_type, classifier)), columns=[], header=True)
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


def average_k_fold(classifier, catalog, entity, binarize, dir_io, k=5):
    predictions, precisions, recalls, fscores = None, [], [], []
    dataset, positive_samples_index = train.build_dataset(
        'training', catalog, entity, dir_io)
    k_fold = StratifiedKFold(n_splits=k, shuffle=True)
    # scikit's stratified k-fold no longer supports multi-label data representation.
    # It expects a binary array instead, so build it based on the positive samples index
    binary_target_variables = dataset.index.map(
        lambda x: 1 if x in positive_samples_index else 0)

    for train_index, test_index in k_fold.split(dataset, binary_target_variables):
        training, test = dataset.iloc[train_index], dataset.iloc[test_index]
        model = workflow.init_model(classifier, binarize)
        model.fit(training, positive_samples_index & training.index)
        preds = model.predict(test)
        p, r, f, _ = _compute_performance(
            positive_samples_index & test.index, preds, len(test))

        if predictions is None:
            predictions = preds
        else:
            predictions |= preds
        precisions.append(p)
        recalls.append(r)
        fscores.append(f)

    return predictions, mean(precisions), std(precisions), mean(recalls), std(recalls), mean(fscores), std(fscores)


def single_k_fold(classifier, catalog, entity, binarize, dir_io, k=5):
    predictions, test_set = None, []
    dataset, positive_samples_index = train.build_dataset(
        'training', catalog, entity, dir_io)
    k_fold = StratifiedKFold(n_splits=k, shuffle=True)
    # scikit's stratified k-fold no longer supports multi-label data representation.
    # It expects a binary array instead, so build it based on the positive samples index
    binary_target_variables = dataset.index.map(
        lambda x: 1 if x in positive_samples_index else 0)

    for train_index, test_index in k_fold.split(dataset, binary_target_variables):
        training, test = dataset.iloc[train_index], dataset.iloc[test_index]
        test_set.append(test)
        model = workflow.init_model(classifier, binarize)
        model.fit(training, positive_samples_index & training.index)
        if predictions is None:
            predictions = model.predict(test)
        else:
            predictions |= model.predict(test)

    test_set = concat(test_set)
    return predictions, _compute_performance(positive_samples_index & test_set.index, predictions, len(test_set))


def _random_split(wd_chunk, target_chunk):
    wd_train, wd_test = train_test_split(wd_chunk, test_size=0.33)
    target_train, target_test = train_test_split(
        target_chunk, test_size=0.33)
    return wd_train, target_train, wd_test, target_test
