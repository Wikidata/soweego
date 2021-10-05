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

__author__ = 'Marco Fossati, Andrea Tupini'
__email__ = 'fossati@spaziodati.eu, tupini07@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs, tupini07'

import logging
import os
from contextlib import redirect_stderr

import numpy as np
import pandas as pd
from mlens.ensemble import SuperLearner
from recordlinkage.adapters import KerasAdapter, SKLearnAdapter
from recordlinkage.base import BaseClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import VotingClassifier as SKVotingClassifier
from sklearn.svm import SVC

from soweego.commons import constants, utils

with redirect_stderr(open(os.devnull, 'w')):
    # When `keras` is imported, it prints a message to stderr
    # saying which backend it's using. To avoid this, we
    # redirect stderr to `devnull` for the statements in this block.
    from keras.callbacks import EarlyStopping, ModelCheckpoint
    from keras.layers import BatchNormalization, Dense
    from keras.models import Sequential
    from keras.wrappers.scikit_learn import KerasClassifier

LOGGER = logging.getLogger(__name__)


# Small wrapper around 'KerasClassifier'. Its only use is to overwrite
# the predict method so that the returned output is (n_samples) instead of
# (n_samples, n_features)
class _KerasClassifierWrapper(KerasClassifier):
    def predict(self, x, **kwargs):
        return super(_KerasClassifierWrapper, self).predict(x, **kwargs)[:, 0]


def _get_proba_sklearn_base_classifier(
    clf: BaseClassifier, features: pd.DataFrame
) -> pd.Series:
    """Returns the probabilities of a positive match by applying the
    classifier to the provided feature vectors"""

    match_class = clf.kernel.classes_[1]

    # Invalid class label
    assert match_class == 1, (
        f'Invalid match class label: {match_class}.'
        'clf.predict_proba() expects the second class '
        'in the trained model to be 1'
    )

    # in the result, rows are classifications and columns are classes.
    # We are in a binary setting, so 2 classes:
    # `0` for non-matches, `1` for matches.
    # We only need the probability of being a match,
    # so we return the second column
    classifications = clf.kernel.predict_proba(features)[:, 1]

    return pd.Series(classifications, index=features.index)


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
            constants.WORK_DIR,
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
            verbose=1,
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
        return self.kernel.predict(values)

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'optimizer={self.optimizer.__class__.__name__}, '
            f'loss={self.loss}, '
            f'metrics={self.metrics})'
        )


class _MLensAdapter(SKLearnAdapter, BaseClassifier):
    """
    Wrapper around :class:`recordlinkage.SKLearnAdapter`, and
    :class:`BaseClassifier` to be used as parent class for any classifier
    which uses as kernel a subclass of :class:`mlens.ensemble.base.BaseEnsemble`.

    This *adapter* correctly implements the *prob* and *_predict* methods so
    the classifier can be properly used with the *recordlinkage* framework.
    """

    def __init__(self, **kwargs):
        super(_MLensAdapter, self).__init__()

    def _check_correct_pred_shape(self, preds: np.ndarray):
        """
        Sanity check to see that the *meta* model in the ensemble
        actually gave as an output two possible classes.
        """
        n_classes = preds.shape[1]
        if n_classes != 2:
            err_msg = (
                "We're doing binary classification and we expect "
                f"probabilities for only two classes, however "
                f"we received '{n_classes}' classes."
            )
            LOGGER.critical(err_msg)
            raise AssertionError(err_msg)

    def prob(self, feature_vectors: pd.DataFrame) -> pd.Series:
        """Classify record pairs and include the probability score
        of being a match.

        :param feature_vectors: a :class:`DataFrame <pandas.DataFrame>`
          computed via record pairs comparison. This should be
          :meth:`recordlinkage.Compare.compute` output.
          See :func:`extract_features() <soweego.linker.workflow.extract_features>`
          for more details
        :return: the classification results
        """

        # mlens `predict` method returns probabilities
        classifications = self.kernel.predict(feature_vectors)
        self._check_correct_pred_shape(classifications)

        # we're only interested in the probability for the positive
        # case
        classifications = classifications[:, 1]

        return pd.Series(classifications, index=feature_vectors.index)

    def _predict(self, features) -> np.ndarray:
        prediction = super(_MLensAdapter, self)._predict(features)
        self._check_correct_pred_shape(prediction)

        prediction = prediction[:, 1]

        # mlens `predict` method returns probabilities. Since we're
        # dealing with a binary classification problem we just get the
        # probabilities for the positive case and round them to [0,1].
        prediction = np.array(list(round(x) for x in prediction))

        return prediction


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

    def prob(self, feature_vectors: pd.DataFrame) -> pd.Series:
        """Classify record pairs and include the probability score
        of being a match.

        :param feature_vectors: a :class:`DataFrame <pandas.DataFrame>`
          computed via record pairs comparison. This should be
          :meth:`recordlinkage.Compare.compute` output.
          See :func:`extract_features() <soweego.linker.workflow.extract_features>`
          for more details
        :return: the classification results
        """

        return _get_proba_sklearn_base_classifier(self, feature_vectors)

    def __repr__(self):
        return f'{self.kernel}'


class RandomForest(SKLearnAdapter, BaseClassifier):
    """A Random Forest classifier.

    This class implements :class:`sklearn.ensemble.RandomForestClassifier`, and receives
    the same parameters.

    It fits multiple decision trees on sub-samples (aka, parts) of the dataset and
    averages the result to get more accuracy and reduce over-fitting.

    The default parameters are:

    - **n_estimators**: 500
    - **criterion**: entropy
    - **max_features**: None
    - **bootstrap**: True
    """

    def __init__(self, *args, **kwargs):
        super(RandomForest, self).__init__()

        kwargs = {**constants.RANDOM_FOREST_PARAMS, **kwargs}
        self.kernel = RandomForestClassifier(*args, **kwargs)

    def prob(self, feature_vectors: pd.DataFrame) -> pd.Series:
        """Classify record pairs and include the probability score
        of being a match.

        :param feature_vectors: a :class:`DataFrame <pandas.DataFrame>`
          computed via record pairs comparison. This should be
          :meth:`recordlinkage.Compare.compute` output.
          See :func:`extract_features() <soweego.linker.workflow.extract_features>`
          for more details
        :return: the classification results
        """

        return _get_proba_sklearn_base_classifier(self, feature_vectors)

    def __repr__(self):
        return f'{self.kernel}'


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

    def __init__(self, num_features, **kwargs):
        super(SingleLayerPerceptron, self).__init__()

        kwargs = {**constants.SINGLE_LAYER_PERCEPTRON_PARAMS, **kwargs}

        self.num_features = num_features
        self.loss = kwargs.get('loss', constants.LOSS)
        self.metrics = kwargs.get('metrics', constants.METRICS)

        self.epochs = kwargs.get('epochs')
        self.batch_size = kwargs.get('batch_size')
        self.activation = kwargs.get('activation')
        self.optimizer = kwargs.get('optimizer')

        model = _KerasClassifierWrapper(
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
        model.add(Dense(1, input_dim=self.num_features, activation=activation))

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

    def __init__(self, num_features, **kwargs):
        super(MultiLayerPerceptron, self).__init__()

        kwargs = {**constants.MULTI_LAYER_PERCEPTRON_PARAMS, **kwargs}

        self.num_features = num_features

        self.loss = kwargs.get('loss', constants.LOSS)
        self.metrics = kwargs.get('metrics', constants.METRICS)

        self.epochs = kwargs.get('epochs')
        self.batch_size = kwargs.get('batch_size')
        self.optimizer = kwargs.get('optimizer')

        self.hidden_activation = kwargs.get('hidden_activation')
        self.output_activation = kwargs.get('output_activation')

        self.hidden_layer_dims = kwargs.get('hidden_layer_dims')

        model = _KerasClassifierWrapper(
            self._create_model,
            optimizer=self.optimizer,
            hidden_activation=self.hidden_activation,
            output_activation=self.output_activation,
            hidden_layer_dims=self.hidden_layer_dims,
        )

        self.kernel = model

    def _create_model(
        self,
        optimizer=None,
        hidden_activation=None,
        output_activation=None,
        hidden_layer_dims=None,
    ):

        if optimizer is None:
            optimizer = self.optimizer

        if hidden_activation is None:
            hidden_activation = self.hidden_activation

        if output_activation is None:
            output_activation = self.output_activation

        if hidden_layer_dims is None:
            hidden_layer_dims = self.hidden_layer_dims

        model = Sequential()

        for i, dim in enumerate(hidden_layer_dims):
            if i == 0:  # is first layer
                model.add(
                    Dense(
                        dim,
                        input_dim=self.num_features,
                        activation=hidden_activation,
                    )
                )
            else:
                model.add(Dense(dim, activation=hidden_activation))

            model.add(BatchNormalization())

        model.add(Dense(1, activation=output_activation))

        model.compile(optimizer=optimizer, loss=self.loss, metrics=self.metrics)

        return model


class VotingClassifier(SKLearnAdapter, BaseClassifier):
    """A basic ensemble classifier which uses a voting procedure to decide the final
    outcome of a prediction.

    This class implements :class:`sklearn.ensemble.VotingClassifier`.

    It combines a set of classifiers and uses majority vote or
    average predicted probabilities to pick the final prediction.
    See scikit's
    `user guide <https://scikit-learn.org/stable/modules/ensemble.html#voting-classifier>`_.

    The parameter `voting` can have as values either **"hard"** or **"soft"**.

    - **hard** - the label predicted by the majority of base classifiers is used as the
        final prediction. Note that this does not return probabilities, only the final
        label.
    - **soft** - the probability that a pair is a match is taken from all base classifiers
        and then averaged. This average is what is returned by the classifier.

    By default `voting=soft`.
    """

    def __init__(self, num_features, **kwargs):
        super(VotingClassifier, self).__init__()

        kwargs = {**constants.VOTING_CLASSIFIER_PARAMS, **kwargs}

        voting = kwargs.pop('voting')

        self.num_features = num_features

        estimators = []
        for clf in constants.CLASSIFIERS_FOR_ENSEMBLE:
            model = utils.init_model(clf, num_features=num_features, **kwargs)

            estimators.append((clf, model.kernel))

        # use as kernel the VotingClassifier coming from sklearn
        self.kernel = SKVotingClassifier(
            estimators=estimators, voting=voting, n_jobs=None
        )

    def prob(self, feature_vectors: pd.DataFrame) -> pd.Series:
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
            'sklearn.ensemble.VotingClassifier.predict_proba() expects the second class '
            'in the trained model to be 1'
        )

        if self.kernel.voting == 'hard':
            classifications = self.kernel.predict(feature_vectors)
        else:
            # get only the probability that pairs are a match
            classifications = self.kernel.predict_proba(feature_vectors)[:, 1]

        return pd.Series(classifications, index=feature_vectors.index)

    def __repr__(self):
        return f'{self.kernel}'


class GatedEnsembleClassifier(_MLensAdapter):
    """Ensemble of classifiers, whose predictions are joined by using
    a further meta-learner, which decides the final output based on the
    prediction of the base classifiers.

    This classifier uses :class:`mlens.ensemble.SuperLearner`
    to implement the *gating* functionality.

    The parameters, and their default values, are:

    - **meta_layer**: Name of the classifier to use as a *meta layer*. By
        default this is `single_layer_perceptron`
    - **folds**: The number of folds to use for cross validation when
        generating the training set for the **meta_layer**. The default
        value for this is `2`.

        For a better explanation of this parameter, see:

        *Polley, Eric C.
        and van der Laan, Mark J., “Super Learner In Prediction” (May 2010).
        U.C. Berkeley Division of Biostatistics Working Paper Series.
        Working Paper 266*
        `<https://biostats.bepress.com/ucbbiostat/paper266/>`_
    """

    def __init__(self, num_features, **kwargs):
        super(GatedEnsembleClassifier, self).__init__()

        kwargs = {**constants.GATED_ENSEMBLE_PARAMS, **kwargs}

        self.num_features = num_features
        self.num_folds = kwargs.pop('folds', 2)
        self.meta_layer = kwargs.pop('meta_layer')

        estimators = []
        for clf in constants.CLASSIFIERS_FOR_ENSEMBLE:
            model = utils.init_model(
                clf, num_features=self.num_features, **kwargs
            )

            estimators.append((clf, model.kernel))

        self.kernel = SuperLearner(verbose=2, n_jobs=1, folds=self.num_folds)

        # use as output the probability of a given class (not just
        # the class itself)
        self.kernel.add(estimators, proba=True)

        self.kernel.add_meta(
            utils.init_model(
                self.meta_layer, len(estimators) * self.num_folds, **kwargs
            ).kernel,
            proba=True,
        )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'num_folds={self.num_folds}, '
            f'meta_layer={self.meta_layer}) '
        )


class StackedEnsembleClassifier(_MLensAdapter):
    """Ensemble of stacked classifiers, meaning that classifiers are arranged in layers
    with the next layer getting as input the output of the last layer.
    The predictions of the final layer are merged with a meta-learner (the same happens for
    ~:class:`soweego.linker.GatedEnsembleClassifier`), which decides the final
    output based on the prediction of the base classifiers.

    This classifier uses :class:`mlens.ensemble.SuperLearner`
    to implement the *stacking* functionality.

    The parameters, and their default values, are:

    - **meta_layer**: Name of the classifier to use as a *meta layer*. By
        default this is `single_layer_perceptron`
    - **folds**: The number of folds to use for cross validation when
        generating the training set for the **meta_layer**. The default
        value for this is `2`.

        For a better explanation of this parameter, see:

        *Polley, Eric C.
        and van der Laan, Mark J., “Super Learner In Prediction” (May 2010).
        U.C. Berkeley Division of Biostatistics Working Paper Series.
        Working Paper 266*
        `<https://biostats.bepress.com/ucbbiostat/paper266/>`_

    """

    def __init__(self, num_features, **kwargs):
        super(StackedEnsembleClassifier, self).__init__()

        kwargs = {**constants.STACKED_ENSEMBLE_PARAMS, **kwargs}

        self.num_features = num_features
        self.num_folds = kwargs.pop('folds', 2)
        self.meta_layer = kwargs.pop('meta_layer')

        def init_estimators(num_features):
            estimators = []
            for clf in constants.CLASSIFIERS_FOR_ENSEMBLE:
                model = utils.init_model(
                    clf, num_features=num_features, **kwargs
                )

                estimators.append((clf, model.kernel))
            return estimators

        self.kernel = SuperLearner(verbose=2, n_jobs=1, folds=self.num_folds)

        l1_estimators = init_estimators(self.num_features)
        self.kernel.add(l1_estimators, proba=True)

        l2_estimators = init_estimators(len(l1_estimators) * self.num_folds)
        self.kernel.add(l2_estimators, proba=True)

        self.kernel.add_meta(
            utils.init_model(
                self.meta_layer, len(l2_estimators) * self.num_folds, **kwargs
            ).kernel,
            proba=True,
        )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'num_folds={self.num_folds}, '
            f'meta_layer={self.meta_layer}) '
        )
