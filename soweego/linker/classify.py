#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Supervised linking."""
import functools
import logging
import os
import re
from csv import DictReader
from typing import Callable, List, Union

import click
import numpy as np
import pandas as pd
import recordlinkage as rl
from numpy import full, nan
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, target_database
from soweego.ingestor import wikidata_bot
from soweego.linker import blocking, classifiers, neural_networks, workflow

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('--upload/--no-upload', default=False, help='Upload links to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-t', '--threshold', default=constants.CONFIDENCE_THRESHOLD,
              help="Probability score threshold, default: 0.5.")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
@click.option('-b', '--block-fields',
              type=click.Choice([
                  constants.NAME,
                  constants.URL,
              ]),
              multiple=True,
              help='Fields on which to perform the post-classification blocking.')
def cli(classifier, target, target_type, block_fields, upload, sandbox, threshold, dir_io):
    """Run a probabilistic linker."""

    # Load model from the specified classifier+target+target_type
    model_path = os.path.join(dir_io, constants.LINKER_MODEL %
                              (target, target_type, classifier))

    results_path = os.path.join(dir_io, constants.LINKER_RESULT %
                                (target, target_type, classifier))

    # Ensure that the model exists
    if not os.path.isfile(model_path):
        err_msg = 'No classifier model found at path: %s ' % model_path
        LOGGER.critical('File does not exist - ' + err_msg)
        raise FileNotFoundError(err_msg)

    # If results path exists then delete it. If not new we results
    # will just be appended to an old results file.
    if os.path.isfile(results_path):
        os.remove(results_path)

    for chunk in execute(target, target_type, model_path, block_fields, threshold, dir_io):
        if upload:
            _upload(chunk, target, sandbox)

        chunk.to_csv(results_path, mode='a', header=False)

    LOGGER.info('Classification complete')


def _upload(predictions, catalog, sandbox):
    links = dict(predictions.to_dict().keys())
    LOGGER.info('Starting addition of links to Wikidata ...')
    wikidata_bot.add_identifiers(links, catalog, sandbox)


def execute(catalog, entity, model, block_fields, threshold, dir_io):
    complete_fv_path = os.path.join(dir_io, constants.COMPLETE_FEATURE_VECTORS %
                                    (catalog, entity, 'classification'))
    complete_wd_path = os.path.join(dir_io, constants.COMPLETE_WIKIDATA_CHUNKS %
                                    (catalog, entity, 'classification'))
    complete_target_path = os.path.join(dir_io, constants.COMPLETE_TARGET_CHUNKS %
                                        (catalog, entity, 'classification'))

    classifier = joblib.load(model)
    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    # check if files exists for these paths. If yes then just
    # preprocess them in chunks instead of recomputing
    if all(os.path.isfile(p) for p in [complete_fv_path,
                                       complete_wd_path,
                                       complete_target_path]):

        LOGGER.info(
            'Using previously cached version of the classification dataset')

        fvectors = pd.read_pickle(complete_fv_path)
        wd_chunks = pd.read_pickle(complete_wd_path)
        target_chunks = pd.read_pickle(complete_target_path)

        _add_missing_feature_columns(classifier, fvectors)

        predictions = classifier.predict(fvectors) if isinstance(
            classifier, rl.SVMClassifier) else classifier.prob(fvectors)

        # perfect block on the specified columns
        predictions = _post_classification_blocking(predictions,
                                                    wd_chunks,
                                                    target_chunks,
                                                    block_fields)

        # if WD link is present and correct then predicion is 1
        if target_chunks.get(constants.URL) is not None:
            predictions = pd.DataFrame(predictions).apply(
                _one_when_wikidata_link_correct, axis=1, args=(target_chunks,))

        yield predictions[predictions >= threshold].drop_duplicates()

    else:

        LOGGER.info('Cached version of the classification dataset not found. '
                    'Creating it from scratch.')

        wd_reader = workflow.build_wikidata(
            'classification', catalog, entity, dir_io)
        wd_generator = workflow.preprocess_wikidata(
            'classification', wd_reader)

        all_feature_vectors = []
        all_wd_chunks = []
        all_target_chunks = []

        for i, wd_chunk in enumerate(wd_generator, 1):
            # TODO Also consider blocking on URLs

            samples = blocking.full_text_query_block(
                'classification', catalog, wd_chunk[constants.NAME_TOKENS],
                i, target_database.get_entity(catalog, entity), dir_io)

            # Build target chunk based on samples
            target_reader = data_gathering.gather_target_dataset(
                'classification', entity, catalog, set(samples.get_level_values(constants.TID)))

            # Preprocess target chunk
            target_chunk = workflow.preprocess_target(
                'classification', target_reader)

            features_path = os.path.join(
                dir_io, constants.FEATURES % (catalog, entity, 'classification', i))

            feature_vectors = workflow.extract_features(
                samples, wd_chunk, target_chunk, features_path)

            # keep features before adding missing features vectors, which may
            # change depending on the classifier.
            all_feature_vectors.append(feature_vectors)
            all_wd_chunks.append(wd_chunk)
            all_target_chunks.append(target_chunk)

            _add_missing_feature_columns(classifier, feature_vectors)

            predictions = classifier.predict(feature_vectors) if isinstance(
                classifier, rl.SVMClassifier) else classifier.prob(feature_vectors)

            # perfect block on the specified columns
            predictions = _post_classification_blocking(predictions,
                                                        wd_chunk,
                                                        target_chunk,
                                                        block_fields)

            # if WD link is present and correct then predicion is 1
            if target_chunk.get(constants.URL) is not None:
                predictions = pd.DataFrame(predictions).apply(
                    _one_when_wikidata_link_correct, axis=1, args=(target_chunk,))

            LOGGER.info('Chunk %d classified', i)

            yield predictions[predictions >= threshold].drop_duplicates()

        # dump all processed chunks as pickled files
        pd.concat(all_feature_vectors, sort=False).to_pickle(complete_fv_path)
        pd.concat(all_wd_chunks, sort=False).to_pickle(complete_wd_path)
        pd.concat(all_target_chunks, sort=False).to_pickle(
            complete_target_path)


def _post_classification_blocking(predictions: pd.Series, wd_chunk: pd.DataFrame, target_chunk: pd.DataFrame,
                                  block_fields: List[str]) -> pd.Series:

    partial_blocking_func = functools.partial(_zero_when_not_exact_match,
                                              wikidata=wd_chunk,
                                              fields=block_fields,
                                              target=target_chunk)

    # only do blocking when there is actually some
    # field to block on
    if block_fields:
        predictions = pd.DataFrame(predictions).apply(
            partial_blocking_func, axis=1)

    return predictions


def _zero_when_not_exact_match(prediction: pd.Series,
                               fields: Union[str, List[str]],
                               wikidata: pd.DataFrame,
                               target: pd.DataFrame) -> float:

    if isinstance(fields, str):
        fields = [fields]

    wd_values, target_values = set(), set()

    qid, tid = prediction.name

    for column in fields:

        if wikidata.get(column) is not None:
            values = wikidata.loc[qid][column]

            if values is not nan:
                wd_values.update(set(values))

        if target.get(column) is not None:
            values = target.loc[tid][column]

            if values is not nan:
                target_values.update(set(values))

    return 0.0 if wd_values.isdisjoint(target_values) else prediction[0]


def _one_when_wikidata_link_correct(prediction, target):
    qid, tid = prediction.name

    urls = target.loc[tid][constants.URL]
    if urls:
        for u in urls:
            if u:
                if 'wikidata' in u:
                    res = re.search(r'(Q\d+)$', u)
                    if res:
                        LOGGER.debug(
                            f"""Changing prediction: {qid}, {tid} --- {u} = {1.0 if qid == res.groups()[
                                0] else 0}, before it was {prediction[0]}""")
                        return 1.0 if qid == res.groups()[0] else 0

    return prediction[0]


def _add_missing_feature_columns(classifier, feature_vectors):
    if isinstance(classifier, rl.NaiveBayesClassifier):
        expected_features = len(classifier.kernel._binarizers)

    elif isinstance(classifier, (classifiers.SVCClassifier, rl.SVMClassifier)):
        expected_features = classifier.kernel.coef_.shape[1]

    elif isinstance(classifier, neural_networks.SingleLayerPerceptron):
        expected_features = classifier.kernel.input_shape[1]

    else:
        err_msg = f'Unsupported classifier: {classifier.__name__}. It should be one of {set(constants.CLASSIFIERS)}'
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    actual_features = feature_vectors.shape[1]
    if expected_features != actual_features:
        LOGGER.info('Feature vectors have %d features, but %s expected %d. Will add missing ones',
                    actual_features, classifier.__class__.__name__, expected_features)
        for i in range(expected_features - actual_features):
            feature_vectors[f'missing_{i}'] = full(
                len(feature_vectors), constants.FEATURE_MISSING_VALUE)
