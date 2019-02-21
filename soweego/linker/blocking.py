#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to index record pairs (read blocking)."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import pandas as pd
from numpy import nan
from recordlinkage import Index

from soweego.commons import constants
from soweego.commons.data_gathering import tokens_fulltext_search

LOGGER = logging.getLogger(__name__)


def train_test_block(wikidata_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.MultiIndex:
    blocking_column = constants.TID

    LOGGER.info("Blocking on column '%s'", blocking_column)

    idx = Index()
    idx.block(blocking_column)
    positive_index = idx.index(wikidata_df, target_df)

    LOGGER.info('Blocking index built')

    return positive_index


def full_text_query_block(wikidata_df: pd.DataFrame, catalog: str, target_entity: constants.DB_ENTITY, dir_io: str) -> pd.MultiIndex:
    samples_path = os.path.join(dir_io, constants.TRAINING_SAMPLES % catalog)

    if os.path.exists(samples_path):
        LOGGER.info("Will reuse existing %s training samples: '%s'",
                    catalog, samples_path)
        tids = pd.read_pickle(samples_path)
    else:
        blocking_column = constants.NAME_TOKENS
        LOGGER.info("Blocking on column '%s' via full-text query ...",
                    blocking_column)

        tids = wikidata_df[blocking_column].dropna().apply(
            _run_full_text_query, args=(target_entity,))
        tids.dropna(inplace=True)

        LOGGER.debug('Candidate target IDs sample:\n%s', tids.sample(5))

    qids_and_tids = []
    for qid, tids in tids.to_dict().items():
        for tid in tids:
            qids_and_tids.append((qid, tid))

    samples_index = pd.MultiIndex.from_tuples(
        qids_and_tids, names=[constants.QID, constants.TID])

    LOGGER.debug('Candidate target IDs index sample:\n%s',
                 samples_index.to_series().sample(5))
    LOGGER.info('Blocking index built')

    return samples_index


def _run_full_text_query(terms: list, target_entity: constants.DB_ENTITY, boolean_mode=False, limit=5):
    tids = set()
    if isinstance(terms, str):
        terms = [terms]
    LOGGER.debug("Full-text query terms: %s", terms)

    for result in tokens_fulltext_search(target_entity, boolean_mode, terms):
        tids.add(result.catalog_id)
        LOGGER.debug('%s result: %s',
                     target_entity.__name__, result)

    if not tids:
        LOGGER.info('No target candidates for source query: %s', terms)
        return nan

    top = list(tids)[:limit]
    LOGGER.debug('Top %d target candidates: %s', limit, top)

    return top
