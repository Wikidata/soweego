#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Supervised linking."""
import logging
import os

import click
import recordlinkage as rl
from numpy import full, nan
from pandas import DataFrame
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, target_database
from soweego.ingestor import wikidata_bot
from soweego.linker import blocking, workflow

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
@click.option('--name-rule/--no-name-rule', default=False,
              help='Activate post-classification rule on full names: links with different full names will be filtered. Default: no.')
@click.option('--upload/--no-upload', default=False, help='Upload links to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-t', '--threshold', default=constants.CONFIDENCE_THRESHOLD,
              help="Probability score threshold, default: 0.5.")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def cli(classifier, target, target_type, name_rule, upload, sandbox, threshold, dir_io):
    """Run a probabilistic linker."""

    # Load model from the specified classifier+target+target_type
    model_path = os.path.join(dir_io, constants.LINKER_MODEL %
                              (target, target_type, classifier))

    # Ensure that the model exists
    if not os.path.isfile(model_path):
        err_msg = 'No classifier model found at path: %s ' % model_path
        LOGGER.critical('File does not exist - ' + err_msg)
        raise FileNotFoundError(err_msg)

    for chunk in execute(target, target_type, model_path, name_rule, threshold, dir_io):
        if upload:
            _upload(chunk, target, sandbox)

        chunk.to_csv(os.path.join(dir_io, constants.LINKER_RESULT %
                                  (target, target_type, classifier)), mode='a', header=True)


def _upload(predictions, catalog, sandbox):
    links = dict(predictions.to_dict().keys())
    LOGGER.info('Starting addition of links to Wikidata ...')
    wikidata_bot.add_identifiers(links, catalog, sandbox)


def execute(catalog, entity, model, name_rule, threshold, dir_io):
    wd_reader = workflow.build_wikidata(
        'classification', catalog, entity, dir_io)
    wd_generator = workflow.preprocess_wikidata('classification', wd_reader)

    classifier = joblib.load(model)
    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)
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

        _add_missing_feature_columns(classifier, feature_vectors)

        if isinstance(classifier, rl.NaiveBayesClassifier):
            predictions = classifier.prob(feature_vectors)
        elif isinstance(classifier, rl.SVMClassifier):
            predictions = classifier.predict(feature_vectors)
        else:
            err_msg = f'Unsupported classifier: {classifier}. It should be one of {set(constants.CLASSIFIERS)}'
            LOGGER.critical(err_msg)
            raise ValueError(err_msg)

        # See https://stackoverflow.com/a/18317089/10719765
        if name_rule:
            LOGGER.info('Applying full names rule ...')
            predictions = DataFrame(predictions).apply(
                _zero_when_different_names, axis=1, args=(wd_chunk, target_chunk))

        LOGGER.info('Chunk %d classified', i)
        yield predictions[predictions >= threshold].drop_duplicates()


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


def _add_missing_feature_columns(classifier, feature_vectors):
    if isinstance(classifier, rl.NaiveBayesClassifier):
        expected_features = len(classifier.kernel._binarizers)
    elif isinstance(classifier, rl.SVMClassifier):
        expected_features = classifier.kernel.coef_.shape[1]
    else:
        err_msg = f'Unsupported classifier: {classifier}. It should be one of {set(constants.CLASSIFIERS)}'
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    actual_features = feature_vectors.shape[1]
    if expected_features != actual_features:
        LOGGER.info('Feature vectors have %d features, but %s expected %d. Will add missing ones',
                    actual_features, classifier.__class__.__name__, expected_features)
        for i in range(expected_features - actual_features):
            feature_vectors[f'missing_{i}'] = full(
                len(feature_vectors), constants.FEATURE_MISSING_VALUE)
