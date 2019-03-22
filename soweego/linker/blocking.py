#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to index record pairs (read blocking)."""
from multiprocessing import Pool
from typing import Tuple, Iterable

from tqdm import tqdm

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import os

import pandas as pd
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


def full_text_query_block(goal: str, catalog: str, wikidata_series: pd.Series, chunk_number: int,
                          target_entity: constants.DB_ENTITY, dir_io: str) -> pd.MultiIndex:
    handle_goal(goal)
    samples_path = os.path.join(
        dir_io, constants.SAMPLES % (catalog, target_entity.__name__, goal, chunk_number))

    if os.path.exists(samples_path):
        LOGGER.info("Will reuse existing %s %s samples index, chunk %d: '%s'",
                    catalog, goal, chunk_number, samples_path)
        return pd.read_pickle(samples_path)

    LOGGER.info(
        "Blocking on column '%s' via full-text query to get all samples ...", wikidata_series.name)

    qids_and_tids = _extract_target_candidates(wikidata_series, target_entity)

    samples_index = pd.MultiIndex.from_tuples(
        qids_and_tids, names=[constants.QID, constants.TID])
    LOGGER.debug('%s %s samples index chunk %d random example:\n%s',
                 catalog, goal, chunk_number, samples_index.to_series().sample(5))

    pd.to_pickle(samples_index, samples_path)
    LOGGER.info("%s %s samples index chunk %d dumped to '%s'",
                catalog, goal, chunk_number, samples_path)

    LOGGER.info('Built blocking index of all samples, chunk %d', chunk_number)
    return samples_index


def _multiprocessing_series_iterator(wikidata_series: pd.Series, target_entity: constants.DB_ENTITY) -> Iterable[
    Tuple[str, str, constants.DB_ENTITY]]:
    for qids, terms in wikidata_series.items():
        yield qids, terms, target_entity


def fulltext_search(qid_terms_target: Tuple[str, list, constants.DB_ENTITY]) -> Iterable[Tuple[str, str]]:
    qid, terms, target_entity = qid_terms_target
    tids = list(map(lambda entity: entity.catalog_id, tokens_fulltext_search(target_entity, False, terms, None, 5)))
    return [(qid, tid) for tid in tids]


def _extract_target_candidates(wikidata_series: pd.Series, target_entity: constants.DB_ENTITY):
    with Pool() as pool:
        for res in tqdm(
                pool.imap_unordered(fulltext_search, _multiprocessing_series_iterator(wikidata_series, target_entity)),
                total=len(wikidata_series)):
            yield from res
