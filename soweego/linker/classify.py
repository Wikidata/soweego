#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Supervised linking."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import logging
import os

import click
import recordlinkage as rl
from pandas import DataFrame, concat, read_json
from sklearn.externals import joblib

from soweego.commons import constants, data_gathering, target_database
from soweego.linker import workflow
from soweego.wikidata.api_requests import get_data_for_linker

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.argument('model', type=click.File())
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared', help="Default: '/app/shared'")
def cli(target, target_type, model, output_dir):
    """Run a probabilistic linker."""
    result = execute(target, target_type, model, output_dir)
    result.to_json(os.path.join(
        output_dir, constants.LINKER_RESULT % target), lines=True)


def execute(catalog, entity, model, dir_io):
    wd_reader, target_reader = build(catalog, entity, dir_io)
    wd = concat([DataFrame(chunk)
                 for chunk in wd_reader], ignore_index=True, sort=False)
    target = concat([DataFrame(chunk)
                     for chunk in target_reader], ignore_index=True, sort=False)
    workflow.preprocess(wd, target)
    candidate_pairs = block(wd, target)
    feature_vectors = workflow.extract_features(candidate_pairs, wd, target)
    classifier = joblib.load(model)
    return classifier.predict(feature_vectors)


def build(catalog, entity, dir_io):
    catalog_pid = target_database.get_pid(catalog)

    # Wikidata
    wd_io_path = os.path.join(dir_io, constants.WD_DATASET_IO % catalog)
    if not os.path.exists(wd_io_path):
        qids = data_gathering.gather_qids(entity, catalog, catalog_pid)
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(qids, url_pids, ext_id_pids_to_urls, wd_io)

    wd_df_reader = read_json(wd_io_path, lines=True,
                             chunksize=1000, orient='index')

    # Target
    target_io_path = os.path.join(
        dir_io, constants.TARGET_DATASET_IO % catalog)
    if not os.path.exists(target_io_path):
        # Get target ids from Wikidata
        qids_and_tids = {}
        data_gathering.gather_target_ids(
            entity, catalog, catalog_pid, qids_and_tids)
        target_ids = set()
        for data in qids_and_tids.values():
            for identifier in data[constants.TID]:
                target_ids.add(identifier)
        # Dataset
        with gzip.open(target_io_path, 'wt') as target_io:
            data_gathering.gather_target_dataset(
                entity, catalog, target_ids, target_io)

    target_df_reader = read_json(
        target_io_path, lines=True, chunksize=1000, orient='index')

    return wd_df_reader, target_df_reader


def block(wikidata_df, target_df):
    # TODO blocking with full-text index query, right now just on WD birth name and Discogs real name
    idx = rl.Index()
    idx.block('birth_name', 'real_name')
    return idx.index(wikidata_df, target_df)


if __name__ == "__main__":
    results = execute('discogs', 'musician', 'discogs_model.pkl', '.')
    print()
