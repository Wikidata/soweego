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
from functools import partial
from multiprocessing import Pool
from typing import Callable, Iterable, Tuple

import pandas as pd
from recordlinkage import Index
from tqdm import tqdm

from soweego.commons import constants, data_gathering
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

    wikidata_series.dropna(inplace=True)
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


def prefect_block_on_column(goal: str, catalog: str, wikidata_series: pd.Series, chunk_number: int,
                            target_entity: constants.DB_ENTITY, dir_io: str, target_column=None) -> pd.MultiIndex:
    handle_goal(goal)

    if not target_column:
        target_column = wikidata_series.name

    # name of the column we're blocking on should correspond with the name of
    # the pd.Series
    column = wikidata_series.name

    # choose the correct `blocking_fn` based on the
    # column we're blocking on.
    # The blocking functions should accept 2 parameters a (target_entity, data_to_block_on)
    if column == constants.NAME_TOKENS:
        # Since `data_gathering.tokens_fulltext_search` accepts more
        # than one parameter we create a partial function, setting the value
        # of all parameters except for the (target_entity, data_to_block_on) ones
        blocking_fn = partial(data_gathering.tokens_fulltext_search,
                              boolean_mode=False,
                              where_clause=None,
                              limit=5)

    else:

        # block on an arbitrary column
        blocking_fn = partial(data_gathering.perfect_column_search,
                              target_column=target_column)

    samples_path = os.path.join(
        dir_io, constants.SAMPLES % (catalog, target_entity.__name__, goal, chunk_number))

    if os.path.exists(samples_path):
        LOGGER.info("Will reuse existing %s %s samples index, chunk %d: '%s'",
                    catalog, goal, chunk_number, samples_path)
        return pd.read_pickle(samples_path)

    LOGGER.info(
        "Blocking on column '%s' perfect match to get all samples ...", target_column)

    wikidata_series.dropna(inplace=True)

    qids_and_tids = []

    with Pool() as pool:

        # this will hold our async processes and the QID
        # they correspond to.
        # We're applying a blocking on each QID
        processes_ = []

        for qid, item_value in wikidata_series.items():
            processes_.append(
                (qid,
                 pool.apply_async(
                     blocking_fn, kwds={
                         'target_entity': target_entity,
                         'to_search': item_value})
                 ))

        # We loop through our async processes
        for qid, async_res in tqdm(processes_):
            matches = async_res.get()

            # For each blocking match found for the `qid` add a tuple
            # (qid, tid) to `qids_and_tids`
            qids_and_tids += [(qid, entity.catalog_id) for entity in matches]

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
    tids = list(map(lambda entity: entity.catalog_id,
                    tokens_fulltext_search(target_entity, False, terms, None, 5)))
    return [(qid, tid) for tid in tids]


def _extract_target_candidates(wikidata_series: pd.Series, target_entity: constants.DB_ENTITY):
    with Pool() as pool:
        for res in tqdm(
                pool.imap_unordered(fulltext_search, _multiprocessing_series_iterator(
                    wikidata_series, target_entity)),
                total=len(wikidata_series)):
            yield from res
