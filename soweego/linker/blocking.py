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

    LOGGER.info(
        "Blocking on column '%s' to get positive samples ...", blocking_column)

    idx = Index()
    idx.block(blocking_column)
    positive_index = idx.index(wikidata_df, target_df)

    LOGGER.info('Built blocking index of positive samples')

    return positive_index


def full_text_query_block(goal: str, catalog: str, wikidata_df: pd.DataFrame, chunk_number: int, target_entity: constants.DB_ENTITY, dir_io: str) -> pd.MultiIndex:
    if goal == 'training':
        samples_path = os.path.join(
            dir_io, constants.TRAINING_SAMPLES % (catalog, chunk_number))
    elif goal == 'classification':
        samples_path = os.path.join(
            dir_io, constants.CLASSIFICATION_SAMPLES % (catalog, chunk_number))
    else:
        LOGGER.critical(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'", goal)
        raise ValueError(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'" % goal)

    if os.path.exists(samples_path):
        LOGGER.info("Will reuse existing %s %s samples: '%s'",
                    catalog, goal, samples_path)
        tids = pd.read_pickle(samples_path)
    else:
        blocking_column = constants.NAME_TOKENS
        LOGGER.info("Blocking on column '%s' via full-text query to get all samples ...",
                    blocking_column)

        tids = wikidata_df[blocking_column].dropna().apply(
            _run_full_text_query, args=(target_entity,))
        tids.dropna(inplace=True)
        LOGGER.debug('%s %s samples random example:\n%s',
                     catalog, goal, tids.sample(5))

        pd.to_pickle(tids, samples_path)
        LOGGER.info("%s %s samples dumped to '%s'",
                    catalog, goal, samples_path)

    qids_and_tids = []
    for qid, tids in tids.to_dict().items():
        for tid in tids:
            qids_and_tids.append((qid, tid))

    samples_index = pd.MultiIndex.from_tuples(
        qids_and_tids, names=[constants.QID, constants.TID])

    LOGGER.debug('%s %s samples index random example:\n%s',
                 catalog, goal, samples_index.to_series().sample(5))
    LOGGER.info('Built blocking index of all samples')

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
