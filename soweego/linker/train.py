#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Train supervised linking algorithms."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os
import sys
from typing import Dict, Tuple

import click
import joblib
import pandas as pd
from keras import backend as K
from recordlinkage.base import BaseClassifier
from sklearn.model_selection import GridSearchCV

from soweego.commons import constants, keys, target_database, utils
from soweego.linker import blocking, workflow

LOGGER = logging.getLogger(__name__)


# Let the user pass extra kwargs to the classifier
# This is for development purposes only, and is not explicitly documented
@click.command(
    context_settings={'ignore_unknown_options': True, 'allow_extra_args': True}
)
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.option(
    '-t',
    '--tune',
    is_flag=True,
    help='Run grid search for hyperparameters tuning.',
)
@click.option(
    '-k',
    '--k-folds',
    default=5,
    help="Number of folds for hyperparameters tuning. Use with '--tune'. "
    "Default: 5.",
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.WORK_DIR,
    help=f'Input/output directory, default: {constants.WORK_DIR}.',
)
@click.pass_context
def cli(ctx, classifier, catalog, entity, tune, k_folds, dir_io):
    """Train a supervised linker.

    Build the training set relevant to the given catalog and entity,
    then train a model with the given classification algorithm.
    """
    kwargs = utils.handle_extra_cli_args(ctx.args)
    if kwargs is None:
        sys.exit(1)

    actual_classifier = constants.CLASSIFIERS[classifier]

    model = execute(actual_classifier, catalog, entity, tune, k_folds, dir_io, **kwargs)

    outfile = os.path.join(
        dir_io,
        constants.LINKER_MODEL.format(catalog, entity, actual_classifier),
    )
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    joblib.dump(model, outfile)

    LOGGER.info("%s model dumped to '%s'", classifier, outfile)

    # Free memory in case of neural networks or
    # ensembles which use them:
    # can be done only after the model dump
    if actual_classifier in (
        keys.SINGLE_LAYER_PERCEPTRON,
        keys.MULTI_LAYER_PERCEPTRON,
        keys.VOTING_CLASSIFIER,
        keys.GATED_CLASSIFIER,
        keys.STACKED_CLASSIFIER,
    ):
        K.clear_session()  # Clear the TensorFlow graph

    LOGGER.info('Training completed')


def execute(
    classifier: str,
    catalog: str,
    entity: str,
    tune: bool,
    k: int,
    dir_io: str,
    **kwargs,
) -> BaseClassifier:
    """Train a supervised linker.

    1. Build the training set relevant to the given catalog and entity
    2. train a model with the given classifier

    :param classifier: ``{'naive_bayes', 'linear_support_vector_machines',
      'support_vector_machines', 'single_layer_perceptron',
      'multi_layer_perceptron'}``.
      A supported classifier
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param tune: whether to run grid search for hyperparameters tuning or not
    :param k: number of folds for hyperparameters tuning.
      It is used only when `tune=True`
    :param dir_io: input/output directory where working files
      will be read/written
    :param kwargs: extra keyword arguments that will be passed to the model
        initialization
    :return: the trained model
    """

    feature_vectors, positive_samples_index = build_training_set(
        catalog, entity, dir_io
    )

    if tune:
        best_params = _grid_search(
            k, feature_vectors, positive_samples_index, classifier, **kwargs
        )

        # TODO find a way to avoid retraining:
        # pass `_grid_search.best_estimator_` to recordlinkage classifiers.
        # See `refit` param in
        # https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html#sklearn.model_selection.GridSearchCV
        return _train(
            classifier, feature_vectors, positive_samples_index, **best_params
        )

    return _train(classifier, feature_vectors, positive_samples_index, **kwargs)


def build_training_set(
    catalog: str, entity: str, dir_io: str
) -> Tuple[pd.DataFrame, pd.MultiIndex]:
    """Build a training set.

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param dir_io: input/output directory where working files
      will be read/written
    :return: the feature vectors and positive samples pair.
      Features are computed by comparing *(QID, catalog ID)* pairs.
      Positive samples are catalog IDs available in Wikidata
    """
    goal = 'training'

    # Wikidata side
    wd_reader = workflow.build_wikidata(goal, catalog, entity, dir_io)
    wd_generator = workflow.preprocess_wikidata(goal, wd_reader)

    positive_samples, feature_vectors = None, None

    for i, wd_chunk in enumerate(wd_generator, 1):
        # Positive samples come from Wikidata
        if positive_samples is None:
            positive_samples = wd_chunk[keys.TID]
        else:
            # We concatenate the current chunk
            # and reset `positive_samples` at each iteration,
            # instead of appending each chunk to a list,
            # then concatenate it at the end of the loop.
            # Reason: keeping multiple yet small pandas objects
            # is less memory-efficient
            positive_samples = pd.concat([positive_samples, wd_chunk[keys.TID]])

        # All samples come from queries to the target DB
        # and include negative ones
        all_samples = blocking.find_samples(
            goal,
            catalog,
            wd_chunk[keys.NAME_TOKENS],
            i,
            target_database.get_main_entity(catalog, entity),
            dir_io,
        )

        # Build target chunk from all samples
        target_reader = workflow.build_target(
            goal, catalog, entity, set(all_samples.get_level_values(keys.TID))
        )
        # Preprocess target chunk
        target_chunk = workflow.preprocess_target(goal, target_reader)

        features_path = os.path.join(
            dir_io, constants.FEATURES.format(catalog, entity, goal, i)
        )

        # Extract features from all samples
        chunk_fv = workflow.extract_features(
            all_samples, wd_chunk, target_chunk, features_path
        )

        if feature_vectors is None:
            feature_vectors = chunk_fv
        else:
            feature_vectors = pd.concat([feature_vectors, chunk_fv], sort=False)

    # Final positive samples index
    positive_samples_index = pd.MultiIndex.from_tuples(
        zip(positive_samples.index, positive_samples),
        names=[keys.QID, keys.TID],
    )

    LOGGER.info('Built positive samples index from Wikidata')

    feature_vectors = feature_vectors.fillna(constants.FEATURE_MISSING_VALUE)

    return feature_vectors, positive_samples_index


def _grid_search(
    k: int,
    feature_vectors: pd.DataFrame,
    positive_samples_index: pd.MultiIndex,
    classifier: str,
    **kwargs,
) -> Dict:
    k_fold, target = utils.prepare_stratified_k_fold(
        k, feature_vectors, positive_samples_index
    )
    model = utils.init_model(classifier, feature_vectors.shape[1], **kwargs)

    grid_search = GridSearchCV(
        model.kernel,
        constants.PARAMETER_GRIDS[classifier],
        scoring='f1',
        n_jobs=-1,
        cv=k_fold,
    )
    grid_search.fit(feature_vectors.to_numpy(), target)

    return grid_search.best_params_


def _train(classifier, feature_vectors, positive_samples_index, **kwargs):
    model = utils.init_model(classifier, feature_vectors.shape[1], **kwargs)

    LOGGER.info('Training a %s ...', classifier)

    model.fit(feature_vectors, positive_samples_index)

    LOGGER.info('Training done')

    return model
