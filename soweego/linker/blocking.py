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
from soweego.linker.workflow import handle_goal

LOGGER = logging.getLogger(__name__)


def train_test_block(wikidata_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.MultiIndex:
    blocking_column = constants.TID

    LOGGER.info(
        "Blocking on column '%s' to get positive samples ...", blocking_column)

    idx = Index()
    idx.block(blocking_column)
    positive_index = idx.index(wikidata_df, target_df)

    LOGGER.info('Built blocking index of positive samples')

    return positive_index


def full_text_query_block(goal: str, catalog: str, wikidata_series: pd.Series, chunk_number: int, target_entity: constants.DB_ENTITY, dir_io: str) -> pd.MultiIndex:
    handle_goal(goal)
    samples_path = os.path.join(
        dir_io, constants.SAMPLES % (catalog, goal, chunk_number))

    if os.path.exists(samples_path):
        LOGGER.info("Will reuse existing %s %s samples index, chunk %d: '%s'",
                    catalog, goal, chunk_number, samples_path)
        return pd.read_pickle(samples_path)

    LOGGER.info(
        "Blocking on column '%s' via full-text query to get all samples ...", wikidata_series.name)

    tids = wikidata_series.dropna().apply(
        _run_full_text_query, args=(target_entity,))
    tids.dropna(inplace=True)
    LOGGER.debug('%s %s samples chunk %d random example:\n%s',
                 catalog, goal, chunk_number, tids.sample(5))

    qids_and_tids = []
    for qid, tids in tids.to_dict().items():
        for tid in tids:
            qids_and_tids.append((qid, tid))

    samples_index = pd.MultiIndex.from_tuples(
        qids_and_tids, names=[constants.QID, constants.TID])
    LOGGER.debug('%s %s samples index chunk %d random example:\n%s',
                 catalog, goal, chunk_number, samples_index.to_series().sample(5))

    pd.to_pickle(samples_index, samples_path)
    LOGGER.info("%s %s samples index chunk %d dumped to '%s'",
                catalog, goal, chunk_number, samples_path)

    LOGGER.info('Built blocking index of all samples, chunk %d', chunk_number)
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
