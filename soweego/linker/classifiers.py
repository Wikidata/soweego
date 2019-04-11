#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Implementation of custom classifiers for use with Record Linkage."""

__author__ = 'Andrea Tupini'
__email__ = 'tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, tupini07'

import logging

from recordlinkage.adapters import SKLearnAdapter
from recordlinkage.base import BaseClassifier
from sklearn.svm import SVC


class SVCClassifier(SKLearnAdapter, BaseClassifier):
    def __init__(self, *args, kernel='linear', probability=True, **kwargs):
        super(SVCClassifier, self).__init__()

        # Add 'kernel' argument to `kwargs`
        kwargs.update({'kernel': kernel,
                       'probability': probability})

        # set the kernel
        self.kernel = SVC(*args, **kwargs)

    def prob(self, x):
        import ipdb
        ipdb.set_trace()
        return self.kernel.predict_proba(x)
