#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run supervised linkers."""
import sys

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os
import re
from typing import Iterator

import click
import pandas as pd
import recordlinkage as rl
from keras import backend as K
from numpy import full, nan
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, keys, target_database
from soweego.ingestor import wikidata_bot
from soweego.linker import blocking, classifiers, neural_networks, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('classifier', type=click.Choice(constants.CLASSIFIERS))
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
    help="Probability score threshold, default: 0.5.",
)
@click.option(
    '-n',
    '--name-rule',
    is_flag=True,
    help='Activate post-classification rule on full names: links with different full names will be filtered.',
)
@click.option(
    '-u',
    '--upload',
    is_flag=True,
    help='Upload links to Wikidata.',
)
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
def cli(classifier, catalog, entity, threshold, name_rule, upload, sandbox,
        dir_io):
    """Run a supervised linker.

    Build the classification set relevant to the given catalog and entity,
    then generate links between Wikidata items and catalog identifiers.

    Output a gzipped CSV file, format: QID,catalog_ID,confidence_score

    You can pass the '-u' flag to upload the output to Wikidata.

    A trained model must exist for the given classifier, catalog, entity.
    To do so, use:

    $ python -m soweego linker train
    """
    model_path, result_path = _handle_io(
        constants.CLASSIFIERS[classifier], catalog, entity, dir_io
    )

    # Exit if the model file doesn't exist
    if model_path is None:
        sys.exit(1)

    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    for chunk in execute(
            catalog, entity, model_path, name_rule, threshold, dir_io
    ):
        if upload:
            _upload(chunk, catalog, entity, sandbox)

        chunk.to_csv(result_path, mode='a', header=False)

    K.clear_session()

    LOGGER.info('Linking completed')


def _handle_io(classifier, catalog, entity, dir_io):
    # Build the output paths upon catalog, entity, and classifier args
    model_path = os.path.join(
        dir_io,
        constants.LINKER_MODEL.format(catalog, entity, classifier)
    )
    result_path = os.path.join(
        dir_io,
        constants.LINKER_RESULT.format(catalog, entity, classifier)
    )

    if not os.path.isfile(model_path):
        LOGGER.critical(
            "Trained model not found at '%s'. "
            "Please run 'python -m soweego linker train %s %s %s'",
            model_path,
            classifier, catalog, entity
        )
        return None, None

    # Delete existing result file,
    # otherwise the current output would be appended to it
    if os.path.isfile(result_path):
        LOGGER.warning(
            "Will delete old output file found at '%s' ...",
            result_path
        )
        os.remove(result_path)

    os.makedirs(os.path.dirname(result_path), exist_ok=True)

    return model_path, result_path


def _upload(predictions, catalog, entity, sandbox):
    links = dict(predictions.to_dict().keys())
    LOGGER.info('Starting addition of links to Wikidata ...')
    wikidata_bot.add_identifiers(links, catalog, entity, sandbox)


def execute(
    catalog: str,
    entity: str,
    model: str,
    name_rule: bool,
    threshold: float,
    dir_io: str,
) -> Iterator[pd.Series]:
    classifier = joblib.load(model)

    wd_reader = workflow.build_wikidata(
        'classification', catalog, entity, dir_io
    )
    wd_generator = workflow.preprocess_wikidata('classification', wd_reader)

    for i, wd_chunk in enumerate(wd_generator, 1):
        samples = blocking.full_text_query_block(
            'classification',
            catalog,
            wd_chunk[keys.NAME_TOKENS],
            i,
            target_database.get_main_entity(catalog, entity),
            dir_io,
        )

        # Build target chunk based on samples
        target_reader = data_gathering.gather_target_dataset(
            'classification',
            entity,
            catalog,
            set(samples.get_level_values(keys.TID)),
        )

        # Preprocess target chunk
        target_chunk = workflow.preprocess_target(
            'classification', target_reader
        )

        # get features
        features_path = os.path.join(
            dir_io, constants.FEATURES % (catalog, entity, 'classification', i)
        )

        feature_vectors = workflow.extract_features(
            samples, wd_chunk, target_chunk, features_path
        )

        _add_missing_feature_columns(classifier, feature_vectors)

        predictions = (
            classifier.predict(feature_vectors)
            if isinstance(classifier, rl.SVMClassifier)
            else classifier.prob(feature_vectors)
        )

        # See https://stackoverflow.com/a/18317089/10719765
        if name_rule:
            LOGGER.info('Applying full names rule ...')
            predictions = pd.DataFrame(predictions).apply(
                _zero_when_different_names,
                axis=1,
                args=(wd_chunk, target_chunk),
            )

        if target_chunk.get(keys.URL) is not None:
            predictions = pd.DataFrame(predictions).apply(
                _one_when_wikidata_link_correct, axis=1, args=(target_chunk,)
            )

        LOGGER.info('Chunk %d classified', i)

        # get all 'confident' predictions
        above_threshold = predictions[predictions >= threshold]

        # yield only those predictions which are not duplicate
        yield above_threshold[~above_threshold.index.duplicated()]


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
        for u in urls:
            if u:
                if 'wikidata' in u:
                    res = re.search(r'(Q\d+)$', u)
                    if res:
                        LOGGER.debug(
                            f"""Changing prediction: {qid}, {tid} --- {u} = {1.0 if qid == res.groups()[
                                0] else 0}, before it was {prediction[0]}"""
                        )
                        return 1.0 if qid == res.groups()[0] else 0

    return prediction[0]


def _add_missing_feature_columns(classifier, feature_vectors: pd.DataFrame):
    if isinstance(classifier, rl.NaiveBayesClassifier):
        expected_features = len(classifier.kernel._binarizers)

    elif isinstance(classifier, (classifiers.SVCClassifier, rl.SVMClassifier)):
        expected_features = classifier.kernel.coef_.shape[1]

    elif isinstance(
            classifier,
            (
                    neural_networks.SingleLayerPerceptron,
                    neural_networks.MultiLayerPerceptron,
            ),
    ):
        expected_features = classifier.kernel.input_shape[1]

    else:
        err_msg = f'Unsupported classifier: {classifier.__name__}. It should be one of {set(constants.CLASSIFIERS)}'
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    actual_features = feature_vectors.shape[1]
    if expected_features != actual_features:
        LOGGER.info(
            'Feature vectors have %d features, but %s expected %d. Will add missing ones',
            actual_features,
            classifier.__class__.__name__,
            expected_features,
        )
        for i in range(expected_features - actual_features):
            feature_vectors[f'missing_{i}'] = full(
                len(feature_vectors), constants.FEATURE_MISSING_VALUE
            )
