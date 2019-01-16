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
from pandas import read_json

from soweego.commons import data_gathering, target_database
from soweego.validator.checks import get_vocabulary
from soweego.wikidata.api_requests import get_data_for_linker

LOGGER = logging.getLogger(__name__)
WD_IO_FILENAME = 'wikidata_%s_dataset.jsonl.gz'
TARGET_IO_FILENAME = '%s_dataset.jsonl.gz'
WD_DF_FILEOUT = 'wikidata_%s_dataset.pkl.gz'


# TODO how to get the whole dataframe: wd_df = pd.concat([pd.DataFrame(chunk) for chunk in wd_df_reader], ignore_index=True, sort=False)

@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared', help="Default: '/app/shared'")
def cli(target, target_type, output_dir):
    """Supervised linking."""
    wikidata_df, target_df = build(target, target_type, output_dir)


def build(catalog, entity, dirout):
    catalog_terms = get_vocabulary(catalog)
    catalog_pid = catalog_terms['pid']

    # Wikidata
    wd_io_path = os.path.join(dirout, WD_IO_FILENAME % catalog)
    if not os.path.exists(wd_io_path):
        qids = data_gathering.gather_qids(entity, catalog, catalog_pid)
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(qids, url_pids, ext_id_pids_to_urls, wd_io)

    wd_df_reader = read_json(wd_io_path, lines=True,
                             chunksize=1000, orient='index')

    # Target
    target_io_path = os.path.join(dirout, TARGET_IO_FILENAME % catalog)
    if not os.path.exists(target_io_path):
        # Get ids from Wikidata
        qids_and_ids = {}
        data_gathering.gather_target_ids(
            entity, catalog, catalog_pid, qids_and_ids)
        target_ids = set()
        for data in qids_and_ids.values():
            for identifier in data['identifiers']:
                target_ids.add(identifier)
        # Dataset
        with gzip.open(target_io_path, 'wt') as target_io:
            data_gathering.gather_target_dataset(
                entity, catalog, target_ids, target_io)

    target_df_reader = read_json(
        target_io_path, lines=True, chunksize=1000, orient='index')

    return wd_df_reader, target_df_reader


if __name__ == "__main__":
    build('imdb', 'actor', '.')
    # data_gathering.gather_target_identifiers('actor', 'imdb', 'P345', {})
