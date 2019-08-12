#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run supervised linkers."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os
import sys
from re import search
from typing import Iterator, Tuple

import click
import joblib
import pandas as pd
import recordlinkage as rl
from keras import backend as K
from numpy import full, nan

from soweego.commons import constants, keys, target_database
from soweego.ingester import wikidata_bot
from soweego.linker import blocking, classifiers, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(
    list(constants.CLASSIFIERS.keys()) + ['all'])
                )
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-t',
    '--threshold',
    default=constants.CONFIDENCE_THRESHOLD,
    help=f"Probability score threshold, default: {constants.CONFIDENCE_THRESHOLD}.",
)
@click.option(
    '-n',
    '--name-rule',
    is_flag=True,
    help='Activate post-classification rule on full names: links with different full names will be filtered.',
)
@click.option('-u', '--upload', is_flag=True, help='Upload links to Wikidata.')
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def cli(
        classifier, catalog, entity, threshold, name_rule, upload, sandbox, dir_io
):
    """Run a supervised linker.

    Build the classification set relevant to the given catalog and entity,
    then generate links between Wikidata items and catalog identifiers.

    Output a gzipped CSV file, format: QID,catalog_ID,confidence_score

    You can pass the '-u' flag to upload the output to Wikidata.

    A trained model must exist for the given classifier, catalog, entity.
    To do so, use:

    $ python -m soweego linker train
    """
    if classifier == "all":
        _run_for_all(catalog, dir_io, entity, name_rule, sandbox, threshold, upload)
    else:
        _run_for_one(catalog, classifier, dir_io, entity, name_rule, sandbox, threshold, upload)

    LOGGER.info('Linking completed')


def _run_for_all(catalog, dir_io, entity, name_rule, sandbox, threshold, upload):
    """
    Runs the `linking` procedure using all available classifiers
    """

    # ensure that models for all classifiers exist, and directly get the model
    # and results path
    available_classifiers = []
    for classifier_name in list(set(constants.CLASSIFIERS.values())):
        model_path, result_path = _handle_io(
            classifier_name, catalog, entity, dir_io
        )
        # Exit if the model file doesn't exist
        if model_path is None:
            sys.exit(1)

        available_classifiers.append((classifier_name, model_path, result_path))

    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    for wd_chunk, target_chunk, feature_vectors in _classification_set_generator(catalog,
                                                                                 entity,
                                                                                 dir_io):
        # predict the current chunk with all classifiers
        for classifier_name, model_path, result_path in available_classifiers:
            classifier = joblib.load(model_path)

            # The classification set must have the same feature space
            # as the training one
            _add_missing_feature_columns(classifier, feature_vectors)

            predictions = (
                # LSVM doesn't support probability scores
                classifier.predict(feature_vectors)
                if isinstance(classifier, rl.SVMClassifier)
                else classifier.prob(feature_vectors)
            )

            predictions = _apply_linking_rules(name_rule,
                                               predictions,
                                               target_chunk,
                                               wd_chunk)

            (_get_unique_predictions_above_threshold(predictions, threshold)
             .to_csv(result_path, mode='a', header=False))

    # Once we have all the classification sets we can proceed to mix them
    # as desired

    # union
    # intersection
    # average

    K.clear_session()  # Clear the TensorFlow graph


def _run_for_one(catalog, classifier, dir_io, entity, name_rule, sandbox, threshold, upload):
    """
    Runs the `linking` procedure for only one classifier
    """

    actual_classifier = constants.CLASSIFIERS[classifier]

    model_path, result_path = _handle_io(
        actual_classifier, catalog, entity, dir_io
    )
    # Exit if the model file doesn't exist
    if model_path is None:
        sys.exit(1)

    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    for i, chunk in enumerate(
            execute(model_path, catalog, entity, threshold, name_rule, dir_io)
    ):
        chunk.to_csv(result_path, mode='a', header=False)

        if upload:
            _upload(chunk, i, catalog, entity, sandbox)

    # Free memory in case of neural networks:
    # can be done only after classification
    if actual_classifier in (
            keys.SINGLE_LAYER_PERCEPTRON,
            keys.MULTI_LAYER_PERCEPTRON,
    ):
        K.clear_session()  # Clear the TensorFlow graph


def execute(
        model_path: str,
        catalog: str,
        entity: str,
        threshold: float,
        name_rule: bool,
        dir_io: str,
) -> Iterator[pd.Series]:
    """Run a supervised linker.

    1. Build the classification set relevant to the given catalog and entity
    2. generate links between Wikidata items and catalog identifiers

    :param model_path: path to a trained model file
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param threshold: minimum confidence score for generated links.
      Those below this value are discarded.
      Must be a float between 0 and 1
    :param name_rule: whether to enable the rule on full names or not:
      if *True*, links with different full names
      are discarded after classification
    :param dir_io: input/output directory where working files
      will be read/written
    :return: the generator yielding chunks of links
    """
    classifier = joblib.load(model_path)

    for wd_chunk, target_chunk, feature_vectors in _classification_set_generator(catalog,
                                                                                 entity,
                                                                                 dir_io):
        # The classification set must have the same feature space
        # as the training one
        _add_missing_feature_columns(classifier, feature_vectors)

        predictions = (
            # LSVM doesn't support probability scores
            classifier.predict(feature_vectors)
            if isinstance(classifier, rl.SVMClassifier)
            else classifier.prob(feature_vectors)
        )

        predictions = _apply_linking_rules(name_rule,
                                           predictions,
                                           target_chunk,
                                           wd_chunk)

        yield _get_unique_predictions_above_threshold(predictions, threshold)


def _classification_set_generator(catalog, entity, dir_io) -> Iterator[Tuple[pd.DataFrame,
                                                                             pd.DataFrame,
                                                                             pd.DataFrame]]:
    goal = 'classification'

    # Wikidata side
    wd_reader = workflow.build_wikidata(goal, catalog, entity, dir_io)
    wd_generator = workflow.preprocess_wikidata(goal, wd_reader)

    for i, wd_chunk in enumerate(wd_generator, 1):
        # Collect samples via queries to the target DB
        samples = blocking.find_samples(
            goal,
            catalog,
            wd_chunk[keys.NAME_TOKENS],
            i,
            target_database.get_main_entity(catalog, entity),
            dir_io,
        )

        # Build target chunk from samples
        target_reader = workflow.build_target(
            goal, catalog, entity, set(samples.get_level_values(keys.TID))
        )

        # Preprocess target chunk
        target_chunk = workflow.preprocess_target(goal, target_reader)

        # Extract features
        features_path = os.path.join(
            dir_io, constants.FEATURES.format(catalog, entity, goal, i)
        )
        feature_vectors = workflow.extract_features(
            samples, wd_chunk, target_chunk, features_path
        )

        yield wd_chunk, target_chunk, feature_vectors

        LOGGER.info('Chunk %d classified', i)


def _apply_linking_rules(name_rule, predictions, target_chunk, wd_chunk):
    # Full name rule: if names differ, it's not a link
    if name_rule:
        LOGGER.info('Applying full names rule ...')
        predictions = pd.DataFrame(predictions).apply(
            _zero_when_different_names,
            axis=1,
            args=(wd_chunk, target_chunk),
        )
    # Wikidata URL rule: if the target ID has a Wikidata URL, it's a link
    if target_chunk.get(keys.URL) is not None:
        predictions = pd.DataFrame(predictions).apply(
            _one_when_wikidata_link_correct, axis=1, args=(target_chunk,)
        )
    return predictions


def _get_unique_predictions_above_threshold(predictions, threshold) -> pd.DataFrame:
    # Filter by threshold
    above_threshold = predictions[predictions >= threshold]

    # Remove duplicates
    return above_threshold[~above_threshold.index.duplicated()]


def _handle_io(classifier, catalog, entity, dir_io):
    # Build the output paths upon catalog, entity, and classifier args
    model_path = os.path.join(
        dir_io, constants.LINKER_MODEL.format(catalog, entity, classifier)
    )
    result_path = os.path.join(
        dir_io, constants.LINKER_RESULT.format(catalog, entity, classifier)
    )

    if not os.path.isfile(model_path):
        LOGGER.critical(
            "Trained model not found at '%s'. "
            "Please run 'python -m soweego linker train %s %s %s'",
            model_path,
            classifier,
            catalog,
            entity,
        )
        return None, None

    # Delete existing result file,
    # otherwise the current output would be appended to it
    if os.path.isfile(result_path):
        LOGGER.warning(
            "Will delete old output file found at '%s' ...", result_path
        )
        os.remove(result_path)

    os.makedirs(os.path.dirname(result_path), exist_ok=True)

    return model_path, result_path


def _upload(chunk, chunk_number, catalog, entity, sandbox):
    links = dict(chunk.to_dict().keys())

    LOGGER.info(
        'Starting upload of links to Wikidata, chunk %d ...', chunk_number
    )

    wikidata_bot.add_identifiers(links, catalog, entity, sandbox)

    LOGGER.info('Upload to Wikidata completed, chunk %d', chunk_number)


def _add_missing_feature_columns(classifier, feature_vectors: pd.DataFrame):
    # Handle amount of features depending on the classifier
    if isinstance(classifier, rl.NaiveBayesClassifier):
        # This seems to be the only easy way for Naïve Bayes
        expected_features = len(classifier.kernel._binarizers)

    elif isinstance(classifier, (classifiers.SVCClassifier, rl.SVMClassifier)):
        expected_features = classifier.kernel.coef_.shape[1]

    elif isinstance(
            classifier,
            (classifiers.SingleLayerPerceptron, classifiers.MultiLayerPerceptron),
    ):
        expected_features = classifier.kernel.input_shape[1]

    else:
        err_msg = (
            f'Unsupported classifier: {classifier.__name__}. '
            f'It should be one of {set(constants.CLASSIFIERS)}'
        )
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    actual_features = feature_vectors.shape[1]

    if expected_features != actual_features:
        LOGGER.info(
            'Feature vectors have %d features, but %s expected %d. '
            'Will add missing ones',
            actual_features,
            classifier.__class__.__name__,
            expected_features,
        )

        for i in range(expected_features - actual_features):
            feature_vectors[f'missing_{i}'] = full(
                len(feature_vectors), constants.FEATURE_MISSING_VALUE
            )


# See https://stackoverflow.com/a/18317089/10719765
# for `pandas` implementation details
def _zero_when_different_names(prediction, wikidata, target):
    wd_names, target_names = set(), set()
    qid, tid = prediction.name

    for column in constants.NAME_FIELDS:
        if wikidata.get(column) is not None:
            values = wikidata.loc[qid][column]

            if values is not nan:
                wd_names.update(set(values))

        if target.get(column) is not None:
            values = target.loc[tid][column]

            if values is not nan:
                target_names.update(set(values))

    return 0.0 if wd_names.isdisjoint(target_names) else prediction[0]


def _one_when_wikidata_link_correct(prediction, target):
    qid, tid = prediction.name
    urls = target.loc[tid][keys.URL]

    if urls:
        for url in urls:
            if url and 'wikidata' in url:
                has_qid = search(constants.QID_REGEX, url)

                if has_qid:
                    new_score = 1.0 if qid == has_qid.group() else 0.0

                    LOGGER.debug(
                        'Wikidata URL detected in %s target ID: %s '
                        'Will update the confidence score from %f to %f',
                        (qid, tid),
                        url,
                        prediction[0],
                        new_score,
                    )
                    return new_score

    return prediction[0]
