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
from pandas import concat
from sklearn.externals import joblib

from soweego.commons import constants, target_database
from soweego.ingestor import wikidata_bot
from soweego.linker import blocking, workflow

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
    for chunk in execute(target, target_type, model, threshold, dir_io):
        if upload:
            _upload(chunk, target, sandbox)
        chunk.to_csv(os.path.join(dir_io, constants.LINKER_RESULT %
                                  target), mode='a', header=True)


def _upload(predictions, catalog, sandbox):
    links = dict(predictions.to_dict().keys())
    LOGGER.info('Starting addition of links to Wikidata ...')
    wikidata_bot.add_identifiers(links, catalog, sandbox)


def execute(catalog, entity, model, threshold, dir_io):
    wd_reader, target_reader = _build(catalog, entity, dir_io)
    wd_generator, target = workflow.preprocess(
        'classification', catalog, wd_reader, target_reader, dir_io)
    # TODO Also consider blocking on URLs
    # FIXME con il blocking sui nomi completi funzia!!! provare con il blocking FT
    classifier = joblib.load(model)
    rl.set_option(*constants.CLASSIFICATION_RETURN_SERIES)

    for i, chunk in enumerate(wd_generator, 1):
        samples = blocking.full_text_query_block(
            'classification', catalog, chunk, i, target_database.get_entity(catalog, entity), dir_io)
        feature_vectors = workflow.extract_features(samples, chunk, target)
        predictions = classifier.prob(feature_vectors)
        LOGGER.info('Chunk %d classified', i)
        yield predictions[predictions >= threshold]


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
