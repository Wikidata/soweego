#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Training set construction for supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os
import sys

import click
import pandas as pd
from keras import backend as K
from pandas import MultiIndex, concat
from typing import Tuple
from sklearn.externals import joblib
from sklearn.model_selection import GridSearchCV

from soweego.commons import (
    constants,
    data_gathering,
    keys,
    target_database,
    utils,
)
from soweego.linker import blocking, workflow

LOGGER = logging.getLogger(__name__)


@click.command(
    context_settings={'ignore_unknown_options': True, 'allow_extra_args': True}
)
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument(
    'target', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'target_type', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '--tune',
    is_flag=True,
    help='Run grid search for hyperparameters tuning. Default: no.',
)
@click.option(
    '-k',
    '--k-folds',
    default=5,
    help="Number of folds for hyperparameters tuning. Use with '--tune' Default: 5.",
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER,
)
@click.pass_context
def cli(ctx, classifier, target, target_type, tune, k_folds, dir_io):
    """Train a probabilistic linker."""
    kwargs = utils.handle_extra_cli_args(ctx.args)
    if kwargs is None:
        sys.exit(1)

    model = execute(
        constants.CLASSIFIERS[classifier],
        target,
        target_type,
        tune,
        k_folds,
        dir_io,
        **kwargs,
    )
    outfile = os.path.join(
        dir_io, constants.LINKER_MODEL % (target, target_type, classifier)
    )
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    joblib.dump(model, outfile)

    K.clear_session()
    LOGGER.info("%s model dumped to '%s'", classifier, outfile)


def execute(classifier, catalog, entity, tune, k, dir_io, **kwargs):
    if tune and classifier in (
            keys.SINGLE_LAYER_PERCEPTRON,
            keys.MULTI_LAYER_PERCEPTRON,
    ):
        # TODO make Keras work with GridSearchCV
        raise NotImplementedError(
            f'Grid search for {classifier} is not supported'
        )

    feature_vectors, positive_samples_index = build_dataset(
        'training', catalog, entity, dir_io
    )

    if tune:
        best_params = _grid_search(
            k, feature_vectors, positive_samples_index, classifier, **kwargs
        )
        # TODO find a way to avoid retraining: pass _grid_search.best_estimator_ to recordlinkage classifiers. See 'refit' param in https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html#sklearn.model_selection.GridSearchCV
        return _train(
            classifier, feature_vectors, positive_samples_index, **best_params
        )

    return _train(classifier, feature_vectors, positive_samples_index, **kwargs)


def _grid_search(
        k, feature_vectors, positive_samples_index, classifier, **kwargs
):
    k_fold, target = utils.prepare_stratified_k_fold(
        k, feature_vectors, positive_samples_index
    )
    model = utils.initialize_classifier(classifier, feature_vectors, **kwargs)
    grid_search = GridSearchCV(
        model.kernel,
        constants.PARAMETER_GRIDS[classifier],
        scoring='f1',
        n_jobs=-1,
        cv=k_fold,
    )
    grid_search.fit(feature_vectors.to_numpy(), target)
    return grid_search.best_params_


def build_dataset(
        goal: str, catalog: str, entity: str, dir_io: str
) -> Tuple[pd.DataFrame, pd.MultiIndex]:
    """
    Creates a dataset for `goal`.

    :param goal: Can be `evaluate` or `train`. Specifies for what end we want
        to use the dataset.
    :param catalog: The external catalog we want to get data for.
    :param entity: The entity in the specified catalog we want to get data for.
    :param dir_io: Input/Output directory where working files will be saved.
    :return: Tuple where the first element is a :class:`pandas.DataFrame` representing
        the features extracted for a given (*QID*, *TID*) pair. The second element of the
        Tuple is a :class:`pandas.MultiIndex`, which represents which (*QID*, *TID*) pairs
        are positive samples (so, which pairs are really the same entity)
    """

    wd_reader = workflow.build_wikidata(goal, catalog, entity, dir_io)
    wd_generator = workflow.preprocess_wikidata(goal, wd_reader)

    positive_samples, feature_vectors = None, None

    for i, wd_chunk in enumerate(wd_generator, 1):
        # Concatenate new positive samples from Wikidata
        # to our current collection
        if positive_samples is None:
            positive_samples = wd_chunk[keys.TID]
        else:
            positive_samples = concat([positive_samples, wd_chunk[keys.TID]])

        # Samples index from Wikidata
        all_samples = blocking.full_text_query_block(
            goal,
            catalog,
            wd_chunk[keys.NAME_TOKENS],
            i,
            target_database.get_main_entity(catalog, entity),
            dir_io,
        )

        # Build target chunk based on samples
        target_reader = data_gathering.gather_target_dataset(
            catalog, entity, set(all_samples.get_level_values(keys.TID))
        )

        # Preprocess target chunk
        target_chunk = workflow.preprocess_target(goal, target_reader)

        # Get features
        features_path = os.path.join(
            dir_io, constants.FEATURES % (catalog, entity, goal, i)
        )

        chunk_fv = workflow.extract_features(
            all_samples, wd_chunk, target_chunk, features_path
        )

        # Concatenate current feature fectors to our collection
        if feature_vectors is None:
            feature_vectors = chunk_fv
        else:
            feature_vectors = concat([feature_vectors, chunk_fv], sort=False)

    positive_samples_index = MultiIndex.from_tuples(
        zip(positive_samples.index, positive_samples),
        names=[keys.QID, keys.TID],
    )

    feature_vectors = feature_vectors.fillna(constants.FEATURE_MISSING_VALUE)

    LOGGER.info('Built positive samples index from Wikidata')
    return feature_vectors, positive_samples_index


def _build_positive_samples_index(wd_reader1):
    LOGGER.info('Building positive samples index from Wikidata ...')
    positive_samples = []
    for chunk in wd_reader1:
        # TODO don't wipe out QIDs with > 1 positive samples!
        tids_series = chunk.set_index(keys.QID)[keys.TID].map(
            lambda cell: cell[0] if isinstance(cell, list) else cell
        )
        positive_samples.append(tids_series)

    positive_samples = concat(positive_samples)
    positive_samples_index = MultiIndex.from_tuples(
        zip(positive_samples.index, positive_samples),
        names=[keys.QID, keys.TID],
    )
    LOGGER.info('Built positive samples index from Wikidata')
    return positive_samples_index


def _train(classifier, feature_vectors, positive_samples_index, **kwargs):
    model = utils.initialize_classifier(classifier, feature_vectors, **kwargs)

    LOGGER.info('Training a %s', classifier)
    model.fit(feature_vectors, positive_samples_index)

    LOGGER.info('Training done')
    return model
