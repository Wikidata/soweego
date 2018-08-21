#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of utilities."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

LOGGER = logging.getLogger(__name__)


def make_buckets(dataset, bucket_size=1000):
    """Slice a dataset into a set of buckets for efficient processing."""
    buckets = [dataset[i*bucket_size:(i+1)*bucket_size]
               for i in range(0, int((len(dataset)/bucket_size+1)))]
    LOGGER.info('Made %s buckets of size %s from a dataset of size %s',
                len(buckets), bucket_size, len(dataset))
    return buckets
