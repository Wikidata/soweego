#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of utilities."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

from sklearn.model_selection import StratifiedKFold

from soweego.commons.keys import MULTI_LAYER_PERCEPTRON, SINGLE_LAYER_PERCEPTRON
from soweego.linker.workflow import init_model

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


def initialize_classifier(classifier, dataset, **kwargs):
    if classifier in (SINGLE_LAYER_PERCEPTRON, MULTI_LAYER_PERCEPTRON):
        model = init_model(classifier, dataset.shape[1], **kwargs)
    else:
        model = init_model(classifier, **kwargs)

    LOGGER.info('Model initialized: %s', model)
    return model


def count_num_lines_in_file(file_) -> int:
    # count number of rows and go back to
    # the beginning of file
    n_rows = len(file_.readlines())
    file_.seek(0)

    return n_rows
