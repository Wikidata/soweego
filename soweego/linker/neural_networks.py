#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Neural network classifiers."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging

from keras.layers import Dense
from keras.models import Sequential
from recordlinkage.adapters import KerasAdapter
from recordlinkage.base import BaseClassifier

LOGGER = logging.getLogger(__name__)


class SingleLayerPerceptron(KerasAdapter, BaseClassifier):
    """A single-layer perceptron classifier."""

    def __init__(self, input_dimension):
        super(SingleLayerPerceptron, self).__init__()

        model = Sequential()
        model.add(Dense(1, input_dim=input_dimension, activation='sigmoid'))
        model.compile(
            optimizer='adam',
            loss='binary_crossentropy',
            metrics=['accuracy']
        )

        self.kernel = model

