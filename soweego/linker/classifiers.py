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
import logging
import os
from contextlib import redirect_stderr

import pandas as pd
from keras.wrappers.scikit_learn import KerasClassifier
from recordlinkage.adapters import KerasAdapter, SKLearnAdapter
from recordlinkage.base import BaseClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.svm import SVC

from soweego.commons import constants, utils

__author__ = 'Marco Fossati, Andrea Tupini'
__email__ = 'fossati@spaziodati.eu, tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs, tupini07'

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

        kwargs = {**constants.RANDOM_FOREST_PARAMS, **kwargs}
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


class VoteClassifier(SKLearnAdapter, BaseClassifier):
    """Basic ensemble classifier which chooses the correct prediction by
    using majority voting (aka 'hard' voting) or chooses the label which has the
    most total probability (the argmax of the sum of predictions),
    aka 'soft' voting.
    """

    def __init__(self, num_features, **kwargs):
        super(VoteClassifier, self).__init__()
        voting = kwargs.get('voting', 'soft')

        estimators = []
        for clf in constants.CLASSIFIERS_FOR_ENSEMBLE:
            model = utils.init_model(clf, num_features=num_features, **kwargs)

            estimators.append((clf, model.kernel))

        self.kernel = VotingClassifier(estimators=estimators, voting=voting)

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
        batch_size: int = None,
        epochs: int = None,
        validation_split: float = constants.VALIDATION_SPLIT,
    ) -> None:

        # if batch size or epochs have not been provided as arguments, and
        # the current instance has them as attributes, then use those. If not
        # then use the defaults defined in constants
        if batch_size is None:
            batch_size = self.batch_size

        if epochs is None:
            epochs = self.epochs

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
            verbose=0,
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

    def _create_model(self, **kwargs):
        raise NotImplementedError(
            'Subclasses need to implement the "create_model" method.'
        )

    def _predict(self, values):
        return self.kernel.predict(values)[:, 0]

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'optimizer={self.optimizer.__class__.__name__}, '
            f'loss={self.loss}, '
            f'metrics={self.metrics}, '
            f'config={self._create_model().get_config()})'
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

        kwargs = {**constants.SINGLE_LAYER_PERCEPTRON_PARAMS, **kwargs}

        self.input_dim = input_dimension
        self.loss = kwargs.get('loss', constants.LOSS)
        self.metrics = kwargs.get('metrics', constants.METRICS)

        self.epochs = kwargs.get('epochs')
        self.batch_size = kwargs.get('batch_size')
        self.activation = kwargs.get('activation')
        self.optimizer = kwargs.get('optimizer')

        model = KerasClassifier(
            self._create_model,
            activation=self.activation,
            optimizer=self.optimizer,
        )

        self.kernel = model

    def _create_model(self, activation=None, optimizer=None):
        if optimizer is None:
            optimizer = self.optimizer

        if activation is None:
            activation = self.activation

        model = Sequential()
        model.add(Dense(1, input_dim=self.input_dim, activation=activation))

        model.compile(optimizer=optimizer, loss=self.loss, metrics=self.metrics)

        return model


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

        self.input_dim = input_dimension
        self.loss = kwargs.get('loss', constants.LOSS)
        self.metrics = kwargs.get('metrics', constants.METRICS)
        self.optimizer = kwargs.get('optimizer', 'Nadam')

        self.hidden_activation = kwargs.get(
            'hidden_activation', constants.HIDDEN_ACTIVATION
        )
        self.output_activation = kwargs.get(
            'output_activation', constants.OUTPUT_ACTIVATION
        )

        model = KerasClassifier(
            self._create_model,
            hidden_activation=self.hidden_activation,
            output_activation=self.output_activation,
        )

        self.kernel = model

    def _create_model(
        self,
        optimizer=constants.MLP_OPTIMIZER,
        hidden_activation=constants.HIDDEN_ACTIVATION,
        output_activation=constants.OUTPUT_ACTIVATION,
        hidden_layer_dims=constants.MLP_HIDDEN_LAYERS_DIM,
    ):

        model = Sequential()

        for i, dim in enumerate(hidden_layer_dims):
            if i == 0:  # is first layer
                model.add(
                    Dense(
                        dim,
                        input_dim=self.input_dim,
                        activation=hidden_activation,
                    )
                )
            else:
                model.add(Dense(dim, activation=hidden_activation))

            model.add(BatchNormalization())

        model.add(Dense(1, activation=output_activation))

        model.compile(optimizer=optimizer, loss=self.loss, metrics=self.metrics)

        return model
