#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import click
import recordlinkage as rl
from sklearn.externals import joblib

from soweego.commons import constants, target_database
from soweego.ingestor import wikidata_bot
from soweego.linker import evaluate, workflow

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.argument('model', type=click.Path(exists=True, dir_okay=False, writable=False))
@click.option('--upload/--no-upload', default=True, help='Upload links to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-t', '--threshold', default=constants.CONFIDENCE_THRESHOLD, help="Probability score threshold, default: 0.5.")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default='/app/shared', help="Input/output directory, default: '/app/shared'.")
def cli(target, target_type, model, upload, sandbox, threshold, dir_io):
    """Run a probabilistic linker."""
    result = execute(target, target_type, model, threshold, dir_io)
    if upload:
        _upload(result, target, sandbox)
    result.to_csv(os.path.join(dir_io, constants.LINKER_RESULT %
                               target), header=True)


def _upload(predictions, catalog, sandbox):
    links = dict(predictions.to_dict().keys())
    LOGGER.info('Starting addition of links to Wikidata ...')
    wikidata_bot.add_identifiers(links, catalog, sandbox)


def execute(catalog, entity, model, threshold, dir_io):
    wd_reader, target_reader = _build(catalog, entity, dir_io)
    wd, target = workflow.preprocess(
        'classification', wd_reader, target_reader)
    candidate_pairs = _block(wd, target)
    feature_vectors = workflow.extract_features(candidate_pairs, wd, target)
    predictions = _classify(model, feature_vectors)
    return predictions[predictions >= threshold]


def _classify(model, feature_vectors):
    classifier = joblib.load(model)
    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)
    return classifier.prob(feature_vectors)


def _build(catalog, entity, dir_io):
    LOGGER.info("Building %s %s classification set, I/O directory: '%s'",
                catalog, entity, dir_io)

    # Wikidata
    wd_df_reader, qids_and_tids = workflow.build_wikidata(
        'classification', catalog, entity, dir_io)

    # Target
    target_df_reader = workflow.build_target(
        'classification', catalog, entity, qids_and_tids, dir_io)

    return wd_df_reader, target_df_reader


def _block(wikidata_df, target_df):
    idx = rl.Index()
    # TODO Blocking with full-text index query, right now hack on WD birth name and Discogs real name
    # TODO Also consider blocking on URLs
    # There are 15 items with multiple birth names, and blocking won't work
    wikidata_df['birth_name'] = wikidata_df['birth_name'].map(
        lambda cell: ' '.join(cell) if isinstance(cell, list) else cell)
    idx.block('birth_name', 'real_name')
    return idx.index(wikidata_df, target_df)
