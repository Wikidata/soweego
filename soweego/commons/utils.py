#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of utilities."""
import recordlinkage as rl


__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

from sklearn.model_selection import StratifiedKFold

from soweego.commons import keys, constants
from soweego.linker import classifiers, neural_networks

LOGGER = logging.getLogger(__name__)


def handle_extra_cli_args(args):
    kwargs = {}
    for extra_arg in args:
        try:
            key, value = extra_arg.split('=')
            try:
                # Try to cast values that should be floats
                kwargs[key] = float(value)
            except ValueError:
                # Fall back to string if the value can't be cast
                kwargs[key] = value
        except ValueError:
            LOGGER.critical(
                "Bad format for extra argument '%s'. It should be 'argument=value'",
                extra_arg,
            )
            return None
    return kwargs


def make_buckets(dataset, bucket_size=1000):
    """Slice a dataset into a set of buckets for efficient processing."""
    buckets = [
        dataset[i * bucket_size : (i + 1) * bucket_size]
        for i in range(0, int((len(dataset) / bucket_size + 1)))
    ]
    LOGGER.info(
        'Made %s buckets of size %s from a dataset of size %s',
        len(buckets),
        bucket_size,
        len(dataset),
    )
    return buckets


def prepare_stratified_k_fold(k, dataset, positive_samples_index):
    k_fold = StratifiedKFold(n_splits=k, shuffle=True, random_state=610)
    # scikit's stratified k-fold no longer supports multi-label data representation.
    # It expects a binary array instead, so build it based on the positive samples index
    binary_target_variables = dataset.index.map(
        lambda x: 1 if x in positive_samples_index else 0
    )
    return k_fold, binary_target_variables


def init_model(classifier, num_features, **kwargs):
    if classifier is keys.NAIVE_BAYES:
        model = rl.NaiveBayesClassifier(**kwargs)

    elif classifier is keys.LINEAR_SVM:
        model = rl.SVMClassifier(**kwargs)

    elif classifier is keys.SVM:
        model = classifiers.SVCClassifier(**kwargs)

    elif classifier is keys.SINGLE_LAYER_PERCEPTRON:
        model = neural_networks.SingleLayerPerceptron(num_features, **kwargs)

    elif classifier is keys.MULTI_LAYER_PERCEPTRON:
        model = neural_networks.MultiLayerPerceptron(num_features, **kwargs)

    else:
        err_msg = (
            f'Unsupported classifier: {classifier}. '
            f'It should be one of {set(constants.CLASSIFIERS)}'
        )
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    LOGGER.info('Model initialized: %s', model)

    return model


def count_num_lines_in_file(file_) -> int:
    # count number of rows and go back to
    # the beginning of file
    n_rows = len(file_.readlines())
    file_.seek(0)

    return n_rows


def handle_goal(goal):
    if goal not in ('training', 'classification'):
        err_msg = (
            f"Invalid 'goal' parameter: {goal}. "
            f"It should be 'training' or 'classification'"
        )

        LOGGER.critical(err_msg)
        raise ValueError(err_msg)
