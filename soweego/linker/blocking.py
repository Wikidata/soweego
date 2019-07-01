#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Custom blocking technique for the
`Record Linkage Toolkit <https://recordlinkage.readthedocs.io/>`_,
where blocking stands for *record pairs indexing*.

In a nutshell, blocking means finding *candidate pairs* suitable for comparison:
this is essential to avoid blind comparison of all records, thus reducing the
overall complexity of the task.
In a supervised learning scenario, this translates into finding relevant
training and classification *samples*.

Given a Wikidata :class:`pandas.Series` (dataset column),
this technique finds samples through
`full-text search <https://mariadb.com/kb/en/library/full-text-index-overview/>`_
in natural language mode against the target catalog database.

Target catalog identifiers of the output :class:`pandas.MultiIndex` are also
passed to :func:`build_target() <soweego.linker.workflow.build_target>`
for building the actual target dataset.
"""

import logging
import os
from typing import Iterable, Tuple

import pandas as pd
from recordlinkage import Index
from tqdm import tqdm

from soweego.commons import constants, keys
from soweego.commons.data_gathering import tokens_fulltext_search
from soweego.commons.utils import check_goal_value

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

LOGGER = logging.getLogger(__name__)


def find_samples(
    goal: str,
    catalog: str,
    wikidata_column: pd.Series,
    chunk_number: int,
    target_db_entity: constants.DB_ENTITY,
    dir_io: str,
) -> pd.MultiIndex:
    """Build a blocking index by looking up target catalog identifiers given a
    Wikidata dataset column. A meaningful column should hold strings.

    Under the hood, run
    `full-text search <https://mariadb.com/kb/en/library/full-text-index-overview/>`_
    in *natural language mode* against the target catalog database.

    This function uses multithreaded parallel processing.

    :param goal: ``{'training', 'classification'}``.
      Whether the samples are for training or classification
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param wikidata_column: a Wikidata dataset column holding values suitable
      for full-text search against the target database
    :param chunk_number: which Wikidata chunk will undergo blocking.
      Typically returned by calling :func:`enumerate` over
      :func:`preprocess_wikidata() <soweego.linker.workflow.preprocess_wikidata>`
    :param target_db_entity: an ORM entity (AKA table) of the target catalog
      database that full-text search should aim at
    :param dir_io: input/output directory where index chunks
      will be read/written
    :return: the blocking index holding candidate pairs
    """
    check_goal_value(goal)

    samples_path = os.path.join(
        dir_io,
        constants.SAMPLES.format(
            catalog, target_db_entity.__name__, goal, chunk_number
        ),
    )

    # Early return cached samples, for development purposes
    if os.path.isfile(samples_path):
        LOGGER.info(
            "Will reuse existing %s %s samples index, chunk %d: '%s'",
            catalog,
            goal,
            chunk_number,
            samples_path,
        )
        return pd.read_pickle(samples_path)

    LOGGER.info(
        "Blocking on Wikidata column '%s' "
        "via full-text search to find all samples ...",
        wikidata_column.name,
    )

    wikidata_column.dropna(inplace=True)

    samples = _fire_queries(
        wikidata_column, target_db_entity
    )
    samples_index = pd.MultiIndex.from_tuples(
        samples, names=[keys.QID, keys.TID]
    )

    LOGGER.debug(
        '%s %s samples index chunk %d random example:\n%s',
        catalog,
        goal,
        chunk_number,
        samples_index.to_series().sample(5),
    )

    os.makedirs(os.path.dirname(samples_path), exist_ok=True)
    pd.to_pickle(samples_index, samples_path)

    LOGGER.info(
        "%s %s samples index chunk %d dumped to '%s'",
        catalog,
        goal,
        chunk_number,
        samples_path,
    )

    LOGGER.info('Built blocking index of all samples, chunk %d', chunk_number)

    return samples_index


def _query_generator(
    wikidata_column: pd.Series, target_db_entity: constants.DB_ENTITY
) -> Iterable[Tuple[str, str, constants.DB_ENTITY]]:
    for qid, values in wikidata_column.items():
        yield qid, values, target_db_entity


def _full_text_search(query: Tuple[str, list, constants.DB_ENTITY], boolean_mode: bool = False, limit: int = 5) -> Iterable[Tuple[str, str]]:
    qid, query_terms, target_db_entity = query
    tids = set(
        map(
            lambda entity: entity.catalog_id,
            tokens_fulltext_search(
                target_db_entity, boolean_mode, query_terms, limit=limit
            ),
        )
    )
    LOGGER.debug(
        'Target ID candidates: %s - Query terms: %s', tids, query_terms
    )

    return [(qid, tid) for tid in tids]


def _fire_queries(
    wikidata_column: pd.Series, target_db_entity: constants.DB_ENTITY
):
    for query in tqdm(
        _query_generator(wikidata_column, target_db_entity),
        total=len(wikidata_column),
    ):
        yield from _full_text_search(query)
