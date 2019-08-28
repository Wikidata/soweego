#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A set of custom supervised classifiers suitable for the
`Record Linkage Toolkit <https://recordlinkage.readthedocs.io/>`_.
It includes
`neural networks <https://en.wikipedia.org/wiki/Artificial_neural_network>`_ and
`support-vector machines <https://en.wikipedia.org/wiki/Support-vector_machine>`_.

All classes implement :class:`recordlinkage.base.BaseClassifier`: typically,
you will use its :meth:`fit() <recordlinkage.NaiveBayesClassifier.fit>`,
:meth:`predict() <recordlinkage.NaiveBayesClassifier.predict>`, and
:meth:`prob() <recordlinkage.NaiveBayesClassifier.prob>` methods.
"""
import sys
from collections import namedtuple

import joblib
from sklearn.ensemble import RandomForestClassifier

from soweego.linker import link

__author__ = 'Marco Fossati, Andrea Tupini'
__email__ = 'fossati@spaziodati.eu, tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs, tupini07'

import logging
import os
from contextlib import redirect_stderr

import pandas as pd
from recordlinkage.adapters import KerasAdapter, SKLearnAdapter
from recordlinkage.base import BaseClassifier
from sklearn.svm import SVC

from soweego.commons import constants

with redirect_stderr(open(os.devnull, 'w')):
    # When `keras` is imported, it prints a message to stderr
    # saying which backend it's using. To avoid this, we
    # redirect stderr to `devnull` for the statements in this block.
    from keras.callbacks import EarlyStopping, ModelCheckpoint
    from keras.layers import Dense, BatchNormalization
    from keras.models import Sequential

LOGGER = logging.getLogger(__name__)


class SVCClassifier(SKLearnAdapter, BaseClassifier):
    """A support-vector machine classifier.

    This class implements :class:`sklearn.svm.SVC`,
    which is based on the `libsvm <https://www.csie.ntu.edu.tw/~cjlin/libsvm/>`_
    library.

    This classifier differs from
    :class:`recordlinkage.classifiers.SVMClassifier`, which implements
    :class:`sklearn.svm.LinearSVC`, based on the
    `liblinear <https://www.csie.ntu.edu.tw/~cjlin/liblinear/>`_ library.

    Main highlights:

    - output probability scores
    - can use non-linear kernels
    - higher training time (quadratic to the number of samples)

    """

    def __init__(self, *args, **kwargs):
        super(SVCClassifier, self).__init__()

        kwargs['probability'] = kwargs.get('probability', True)

        self.kernel = SVC(*args, **kwargs)

    def prob(self, feature_vectors: pd.DataFrame) -> pd.DataFrame:
        """Classify record pairs and include the probability score
        of being a match.

        :param feature_vectors: a :class:`DataFrame <pandas.DataFrame>`
          computed via record pairs comparison. This should be
          :meth:`recordlinkage.Compare.compute` output.
          See :func:`extract_features() <soweego.linker.workflow.extract_features>`
          for more details
        :return: the classification results
        """
        match_class = self.kernel.classes_[1]
        # Invalid class label
        assert match_class == 1, (
            f'Invalid match class label: {match_class}.'
            'sklearn.svm.SVC.predict_proba() expects the second class '
            'in the trained model to be 1'
        )

        # `SVC.predict_proba` returns a matrix
        # where rows are classifications and columns are classes.
        # We are in a binary setting, so 2 classes:
        # `0` for non-matches, `1` for matches.
        # We only need the probability of being a match,
        # so we return the second column
        classifications = self.kernel.predict_proba(feature_vectors)[:, 1]

        return pd.Series(classifications, index=feature_vectors.index)

    def __repr__(self):
        return f'{self.kernel}'


class RandomForest(SKLearnAdapter, BaseClassifier):
    """A Random Forest classifier.

    This class implements :class:`sklearn.ensemble.RandomForestClassifier`.

    It fits multiple decision trees on sub-samples of the dataset and
    averages the result to get more accuracy and reduce over-fitting.
    """

    def __init__(self, *args, **kwargs):
        super(RandomForest, self).__init__()

        kwargs['n_estimators'] = kwargs.get('n_estimators', 100)
        kwargs['max_features'] = kwargs.get('max_features', 'auto')
        kwargs['bootstrap'] = kwargs.get('bootstrap', True)

        self.kernel = RandomForestClassifier(*args, **kwargs)

    def prob(self, feature_vectors: pd.DataFrame) -> pd.DataFrame:
        """Classify record pairs and include the probability score
        of being a match.

        :param feature_vectors: a :class:`DataFrame <pandas.DataFrame>`
          computed via record pairs comparison. This should be
          :meth:`recordlinkage.Compare.compute` output.
          See :func:`extract_features() <soweego.linker.workflow.extract_features>`
          for more details
        :return: the classification results
        """

        match_class = self.kernel.classes_[1]

        # Invalid class label
        assert match_class == 1, (
            f'Invalid match class label: {match_class}.'
            'sklearn.ensemble.RandomForestClassifier.predict_proba() expects the second class '
            'in the trained model to be 1'
        )

        # in the result, rows are classifications and columns are classes.
        # We are in a binary setting, so 2 classes:
        # `0` for non-matches, `1` for matches.
        # We only need the probability of being a match,
        # so we return the second column
        classifications = self.kernel.predict_proba(feature_vectors)[:, 1]

        return pd.Series(classifications, index=feature_vectors.index)

    def __repr__(self):
        return f'{self.kernel}'


# Base class that implements the training method
# `recordlinkage.adapters.KerasAdapter_fit`,
# shared across neural network implementations.
class _BaseNeuralNetwork(KerasAdapter, BaseClassifier):
    def _fit(
            self,
            feature_vectors: pd.Series,
            answers: pd.Series = None,
            batch_size: int = constants.BATCH_SIZE,
            epochs: int = constants.EPOCHS,
            validation_split: float = constants.VALIDATION_SPLIT,
    ) -> None:
        model_path = os.path.join(
            constants.SHARED_FOLDER,
            constants.NEURAL_NETWORK_CHECKPOINT_MODEL.format(
                self.__class__.__name__
            ),
        )
        os.makedirs(os.path.dirname(model_path), exist_ok=True)

        history = self.kernel.fit(
            x=feature_vectors,
            y=answers,
            validation_split=validation_split,
            batch_size=batch_size,
            epochs=epochs,
            callbacks=[
                EarlyStopping(
                    monitor='val_loss',
                    patience=100,
                    verbose=2,
                    restore_best_weights=True,
                ),
                ModelCheckpoint(model_path, save_best_only=True),
            ],
        )

        LOGGER.info('Fit parameters: %s', history.params)

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'optimizer={self.kernel.optimizer.__class__.__name__}, '
            f'loss={self.kernel.loss}, '
            f'metrics={self.kernel.metrics}, '
            f'config={self.kernel.get_config()})'
        )


class SingleLayerPerceptron(_BaseNeuralNetwork):
    """A single-layer perceptron classifier.

    This class implements a
    `keras.Sequential <https://keras.io/models/sequential/>`_ model
    with the following default architecture:

    - single `Dense <https://keras.io/layers/core/>`_ layer
    - ``sigmoid`` activation function
    - ``adam`` optimizer
    - ``binary_crossentropy`` loss function
    - ``accuracy`` metric for evaluation

    If you want to override the default parameters, you can pass the following
    keyword arguments to the constructor:

    - **activation** - see
      `available activations <https://keras.io/activations/>`_
    - **optimizer** - see
      `optimizers <https://keras.io/optimizers/>`_
    - **loss** - see
      `available loss functions <https://keras.io/losses/>`_
    - **metrics** - see
      `available metrics <https://keras.io/metrics/>`_

    """

    def __init__(self, input_dimension, **kwargs):
        super(SingleLayerPerceptron, self).__init__()

        model = Sequential()
        model.add(
            Dense(
                1,
                input_dim=input_dimension,
                activation=kwargs.get(
                    'activation', constants.OUTPUT_ACTIVATION
                ),
            )
        )

        model.compile(
            optimizer=kwargs.get('optimizer', constants.SLP_OPTIMIZER),
            loss=kwargs.get('loss', constants.LOSS),
            metrics=kwargs.get('metrics', constants.METRICS),
        )

        self.kernel = model


class MultiLayerPerceptron(_BaseNeuralNetwork):
    """A multi-layer perceptron classifier.

    This class implements a
    `keras.Sequential <https://keras.io/models/sequential/>`_ model
    with the following default architecture:

    - `Dense <https://keras.io/layers/core/>`_ layer 1, with
      ``128`` output dimension and ``relu`` activation function
    - `BatchNormalization <https://keras.io/layers/normalization/>`_ layer
    - `Dense <https://keras.io/layers/core/>`_ layer 2, with
      ``32`` output dimension and ``relu`` activation function
    - `BatchNormalization <https://keras.io/layers/normalization/>`_ layer
    - `Dense <https://keras.io/layers/core/>`_ layer 3, with
      ``1`` output dimension and ``sigmoid`` activation function
    - ``adadelta`` optimizer
    - ``binary_crossentropy`` loss function
    - ``accuracy`` metric for evaluation

    If you want to override the default parameters, you can pass the following
    keyword arguments to the constructor:

    - **activations** - a triple with values for
      *(dense layer 1, dense layer 2, dense layer 3)*.
      See `available activations <https://keras.io/activations/>`_
    - **optimizer** - see
      `optimizers <https://keras.io/optimizers/>`_
    - **loss** - see
      `available loss functions <https://keras.io/losses/>`_
    - **metrics** - see
      `available metrics <https://keras.io/metrics/>`_

    """

    def __init__(self, input_dimension, **kwargs):
        super(MultiLayerPerceptron, self).__init__()

        activations = 'activations'
        try:
            first, second, third = kwargs.get(
                activations,
                (
                    constants.HIDDEN_ACTIVATION,
                    constants.HIDDEN_ACTIVATION,
                    constants.OUTPUT_ACTIVATION,
                ),
            )
        except ValueError:
            err_msg = (
                f"Bad value for '{activations}' keyword argument. "
                'A tuple with 3 elements is expected'
            )
            LOGGER.critical(err_msg)
            raise ValueError(err_msg)

        model = Sequential(
            [
                Dense(128, input_dim=input_dimension, activation=first),
                BatchNormalization(),
                Dense(32, activation=second),
                BatchNormalization(),
                Dense(1, activation=third),
            ]
        )

        model.compile(
            optimizer=kwargs.get('optimizer', constants.MLP_OPTIMIZER),
            loss=kwargs.get('loss', constants.LOSS),
            metrics=kwargs.get('metrics', constants.METRICS),
        )

        self.kernel = model
