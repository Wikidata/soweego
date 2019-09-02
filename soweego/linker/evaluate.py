#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Evaluate supervised linking algorithms."""
import json
import logging
import os
import sys
from typing import Tuple

import click
import joblib
import pandas as pd
import recordlinkage as rl
from keras import backend as K
from numpy import mean, std
from pandas import concat
from sklearn.model_selection import GridSearchCV, StratifiedKFold

from soweego.commons import constants, keys, target_database, utils
from soweego.linker import ensembles, train

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

LOGGER = logging.getLogger(__name__)


# Let the user pass extra kwargs to the classifier
# This is for development purposes only, and is not explicitly documented
@click.command(
    context_settings={'ignore_unknown_options': True, 'allow_extra_args': True}
)
@click.argument('classifier', type=click.Choice(constants.EXTENDED_CLASSIFIERS))
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option('-k', '--k-folds', default=5, help="Number of folds, default: 5.")
@click.option(
    '-s',
    '--single',
    is_flag=True,
    help='Compute a single evaluation over all k folds, instead of k '
         'evaluations.',
)
@click.option(
    '-n',
    '--nested',
    is_flag=True,
    help='Compute a nested cross-validation with hyperparameters tuning via '
         'grid search. WARNING: this will take a lot of time.',
)
@click.option(
    '-m',
    '--metric',
    type=click.Choice(constants.PERFORMANCE_METRICS),
    default='f1',
    help="Performance metric for nested cross-validation. "
         "Use with '--nested'. Default: f1.",
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
@click.option(
    '-t',
    '--threshold',
    default=constants.CONFIDENCE_THRESHOLD,
    help=f"Probability score threshold, default: {constants.CONFIDENCE_THRESHOLD}.",
)
@click.option(
    '-jm',
    '--join-method',
    type=(
            click.Choice(constants.SC_AVAILABLE_JOIN),
            click.Choice(constants.SC_AVAILABLE_COMBINE),
    ),
    default=(constants.SC_INTERSECTION, constants.SC_AVERAGE),
    help=(
            f"Way in which the results of 'all' classifiers are merged. The first term can be 'union' or 'intersection' "
            f"and says how the sets of predictions are joined. The second term can be 'average' or 'vote' and specify "
            # TODO Change the default to the actual best once we know which it is
            f"how duplicates predictions are dealt with. Only used when classifier='all'. Default: {(constants.SC_INTERSECTION, constants.SC_AVERAGE)}."
    ),
)
@click.pass_context
def cli(
        ctx,
        classifier,
        catalog,
        entity,
        k_folds,
        single,
        nested,
        metric,
        dir_io,
        threshold,
        join_method,
):
    """Evaluate the performance of a supervised linker.

    By default, run 5-fold cross-validation and
    return averaged performance scores.
    """
    kwargs = utils.handle_extra_cli_args(ctx.args)
    if kwargs is None:
        sys.exit(1)

    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    performance_out, predictions_out = _build_output_paths(
        catalog, entity, classifier, dir_io, join_method
    )

    # -n, --nested
    if nested:
        _run_nested(
            classifier,
            catalog,
            entity,
            k_folds,
            metric,
            kwargs,
            performance_out,
            dir_io,
        )

    # -s, --single
    elif single:
        _run_single(
            classifier,
            catalog,
            entity,
            k_folds,
            kwargs,
            performance_out,
            predictions_out,
            dir_io,
            threshold,
            join_method,
        )

    else:
        # Default: average evaluation over k-fold
        _run_average(
            classifier,
            catalog,
            entity,
            k_folds,
            kwargs,
            performance_out,
            predictions_out,
            dir_io,
            threshold,
            join_method,
        )

    sys.exit(0)


def _build_output_paths(catalog, entity, classifier, dir_io, join_method):
    # If we're getting the result from an 'ensemble' (all classifiers) then
    # use the appropriate output file

    if classifier == keys.ALL_CLASSIFIERS:
        performanceFp = constants.LINKER_JOINED_PERFORMANCE.format(
            catalog, entity, *join_method
        )
        predictionsFp = constants.LINKER_EVALUATION_JOINED_PREDICTIONS.format(
            catalog, entity, *join_method
        )
    else:

        classifier = constants.CLASSIFIERS.get(classifier)

        performanceFp = constants.LINKER_PERFORMANCE.format(
            catalog, entity, classifier
        )
        predictionsFp = constants.LINKER_EVALUATION_PREDICTIONS.format(
            catalog, entity, classifier
        )

    performance_outpath = os.path.join(dir_io, performanceFp)
    predictions_outpath = os.path.join(dir_io, predictionsFp)
    os.makedirs(os.path.dirname(predictions_outpath), exist_ok=True)

    return performance_outpath, predictions_outpath


def _run_average(
        classifier,
        catalog,
        entity,
        k_folds,
        kwargs,
        performance_out,
        predictions_out,
        dir_io,
        threshold,
        join_method,
):
    LOGGER.info('Starting average evaluation over %d folds ...', k_folds)

    predictions, p_mean, p_std, r_mean, r_std, fscore_mean, fscore_std = _average_k_fold(
        constants.EXTENDED_CLASSIFIERS[classifier],
        catalog,
        entity,
        k_folds,
        dir_io,
        join_method,
        threshold,
        **kwargs,
    )

    LOGGER.info(
        'Evaluation done. '
        'Precision: mean = %s; std = %s; '
        'recall: mean = %s; std = %s; '
        'F-score: mean = %s; std = %s',
        p_mean,
        p_std,
        r_mean,
        r_std,
        fscore_mean,
        fscore_std,
    )

    predictions.to_series().to_csv(predictions_out, header=False)
    with open(performance_out, 'w') as out:
        out.write(
            f'Precision:\n'
            f'\tmean = {p_mean}\n'
            f'\tstandard deviation = {p_std}\n'
            f'Recall:\n'
            f'\tmean = {r_mean}\n'
            f'\tstandard deviation = {r_std}\n'
            f'F-score:\n'
            f'\tmean = {fscore_mean}\n'
            f'\tstandard deviation = {fscore_std}\n'
        )

    LOGGER.info(
        "Predictions dumped to '%s', performance dumped to '%s'",
        predictions_out,
        performance_out,
    )


def _run_single(
        classifier,
        catalog,
        entity,
        k_folds,
        kwargs,
        performance_out,
        predictions_out,
        dir_io,
        threshold,
        join_method,
):
    LOGGER.info('Starting single evaluation over %d folds ...', k_folds)

    predictions, (precision, recall, fscore, confusion_matrix) = _single_k_fold(
        constants.EXTENDED_CLASSIFIERS[classifier],
        catalog,
        entity,
        k_folds,
        dir_io,
        join_method,
        threshold,
        **kwargs,
    )

    LOGGER.info('Evaluation done.')

    predictions.to_series().to_csv(predictions_out, header=False)
    with open(performance_out, 'w') as out:
        out.write(
            f'Precision: {precision}\n'
            f'Recall: {recall}\n'
            f'F-score: {fscore}\n'
            f'Confusion matrix:\n{confusion_matrix}\n'
        )

    LOGGER.info(
        "Predictions dumped to '%s', Performance dumped to '%s'",
        predictions_out,
        performance_out,
    )


def _run_nested(
        classifier,
        catalog,
        entity,
        k_folds,
        metric,
        kwargs,
        performance_out,
        dir_io,
):
    LOGGER.warning(
        'You have opted for the slowest evaluation option, '
        'please be patient ...'
    )
    LOGGER.info(
        'Starting nested %d-fold cross-validation with '
        'hyperparameters tuning via grid search ...',
        k_folds,
    )

    # currently at least one of these are None if the classifier is
    # SLP, MLP, or all
    clf = constants.CLASSIFIERS.get(classifier)
    param_grid = constants.PARAMETER_GRIDS.get(clf)

    if param_grid is None:
        err_msg = (
            f'Hyperparameter tuning for classifier "{clf}" is not supported'
        )
        LOGGER.critical(err_msg)
        raise NotImplementedError(err_msg)

    result = _nested_k_fold_with_grid_search(
        clf, param_grid, catalog, entity, k_folds, metric, dir_io, **kwargs
    )

    LOGGER.info('Evaluation done: %s', result)

    # Persist best models
    for k, rdict in enumerate(result, 1):
        model_out = os.path.join(
            dir_io,
            constants.LINKER_NESTED_CV_BEST_MODEL.format(
                catalog, entity, clf, k
            ),
        )

        model = rdict['best_model']  # get model 'instance'
        rdict['best_model'] = model_out  # replace it with model path

        joblib.dump(model, model_out)  # persist instance

        LOGGER.info("Best model for fold %d dumped to '%s'", k, model_out)

    performance_out = performance_out.replace('txt', 'json')
    with open(performance_out, 'w') as out:
        json.dump(result, out, indent=2)

    LOGGER.info("%s performance dumped to '%s'", metric, performance_out)


def _compute_performance(test_index, predictions, test_vectors_size):
    LOGGER.info('Running performance evaluation ...')

    confusion_matrix = rl.confusion_matrix(
        test_index, predictions, total=test_vectors_size
    )
    precision = rl.precision(test_index, predictions)
    recall = rl.recall(test_index, predictions)
    f_score = rl.fscore(confusion_matrix)

    LOGGER.info(
        'Precision: %f - Recall: %f - F-score: %f', precision, recall, f_score
    )
    LOGGER.info('Confusion matrix: %s', confusion_matrix)

    return precision, recall, f_score, confusion_matrix


def _nested_k_fold_with_grid_search(
        classifier, param_grid, catalog, entity, k, scoring, dir_io, **kwargs
):
    dataset, positive_samples_index = train.build_training_set(
        catalog, entity, dir_io
    )
    model = utils.init_model(classifier, dataset.shape[1], **kwargs).kernel

    inner_k_fold, target = utils.prepare_stratified_k_fold(
        k, dataset, positive_samples_index
    )
    outer_k_fold = StratifiedKFold(n_splits=k, shuffle=True, random_state=1269)
    grid_search = GridSearchCV(
        model,
        param_grid,
        scoring=scoring,
        n_jobs=-1,
        cv=inner_k_fold,
        verbose=2,
    )
    result = []

    dataset = dataset.to_numpy()

    for train_index, test_index in outer_k_fold.split(dataset, target):
        # Run grid search
        grid_search.fit(dataset[train_index], target[train_index])

        # Let grid search compute the test score
        test_score = grid_search.score(dataset[test_index], target[test_index])
        best_model = grid_search.best_estimator_

        # Grid search best score is the train score
        result.append({
            f'train_{scoring}': grid_search.best_score_,
            f'test_{scoring}': test_score,
            'best_model': best_model,
            'params': grid_search.best_params_
        })

    return result


def _average_k_fold(
        classifier, catalog, entity, k, dir_io, join_method, threshold, **kwargs
):
    predictions, precisions, recalls, f_scores = None, [], [], []
    dataset, positive_samples_index = train.build_training_set(
        catalog, entity, dir_io
    )
    k_fold, binary_target_variables = utils.prepare_stratified_k_fold(
        k, dataset, positive_samples_index
    )

    for train_index, test_index in k_fold.split(
            dataset, binary_target_variables
    ):
        training, test = dataset.iloc[train_index], dataset.iloc[test_index]

        preds = _init_model_and_get_preds(
            classifier,
            training,
            test,
            positive_samples_index,
            join_method,
            threshold,
            **kwargs,
        )

        K.clear_session()  # Free memory

        p, r, f, _ = _compute_performance(
            positive_samples_index & test.index, preds, len(test)
        )

        if predictions is None:
            predictions = preds
        else:
            predictions |= preds

        precisions.append(p)
        recalls.append(r)
        f_scores.append(f)

    return (
        predictions,
        mean(precisions),
        std(precisions),
        mean(recalls),
        std(recalls),
        mean(f_scores),
        std(f_scores),
    )


def _single_k_fold(
        classifier, catalog, entity, k, dir_io, join_method, threshold, **kwargs
):
    predictions, test_set = None, []
    dataset, positive_samples_index = train.build_training_set(
        catalog, entity, dir_io
    )
    k_fold, binary_target_variables = utils.prepare_stratified_k_fold(
        k, dataset, positive_samples_index
    )

    for train_index, test_index in k_fold.split(
            dataset, binary_target_variables
    ):
        training, test = dataset.iloc[train_index], dataset.iloc[test_index]
        test_set.append(test)

        preds = _init_model_and_get_preds(
            classifier,
            training,
            test,
            positive_samples_index,
            join_method,
            threshold,
            **kwargs,
        )

        K.clear_session()  # Free memory

        if predictions is None:
            predictions = preds
        else:
            predictions |= preds

    test_set = concat(test_set)

    return (
        predictions,
        _compute_performance(
            positive_samples_index & test_set.index, predictions, len(test_set)
        ),
    )


def _init_model_and_get_preds(
        classifier: str,
        training_set: pd.DataFrame,
        test_set: pd.DataFrame,
        positive_samples_index: pd.MultiIndex,
        join_method: Tuple[str, str],
        threshold=0.5,
        **kwargs,
) -> pd.Series:
    LOGGER.debug(
        'Getting predictions for fold using "%s" classifier and a threshold of "%d"',
        classifier,
        threshold,
    )

    def _fit_predict(clsf: str):
        model = utils.init_model(
            clsf, len(training_set.columns), **kwargs  # num features
        )
        model.fit(training_set, positive_samples_index & training_set.index)

        return (
            # LSVM doesn't support probability scores
            model.predict(test_set)
            if isinstance(model, rl.SVMClassifier)
            else model.prob(test_set)
        )

    if classifier == keys.ALL_CLASSIFIERS:
        LOGGER.debug('Joining results with method "%s"', join_method)
        how_to_join, how_to_rem_duplicates = join_method

        ensembles.assert_join_merge_keywords(how_to_join, how_to_rem_duplicates)

        preds = [_fit_predict(m) for m in constants.CLASSIFIERS_FOR_ENSEMBLE]

        preds = ensembles.ensemble_predictions_by_keywords(
            preds, threshold, how_to_join, how_to_rem_duplicates
        )

    else:
        preds = _fit_predict(classifier)

    return preds[preds >= threshold].index
