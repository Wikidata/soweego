#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of techniques to index record pairs (read blocking)."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

import pandas as pd
from numpy import nan
from recordlinkage.base import BaseIndexAlgorithm

from soweego.commons import constants
from soweego.commons.data_gathering import tokens_fulltext_search
from soweego.commons.logging import log_dataframe_info

LOGGER = logging.getLogger(__name__)


# FIXME probabilmente non serve mettere la logica dentro a classi recordlinkage: mettilo in workflow e basta
class FullTextQueryBlock(BaseIndexAlgorithm):
    """Blocking through a full-text query against the given target database."""

    def __init__(self, left_on, target_db_entity, boolean_mode=False, limit=5):
        super(FullTextQueryBlock, self).__init__()
        self.left_on = left_on
        self.target_db_entity = target_db_entity
        self.boolean_mode = boolean_mode
        self.limit = limit

    def __repr__(self):
        return f'<{self.__class__.__name__} left_on={self.left_on}>'

    def _link_index(self, df_a, df_b):
        tids = df_a[self.left_on].map(
            self._run_full_text_query, na_action='ignore')
        LOGGER.debug('Candidate target IDs sample: %s', tids.sample(5))
        # FIXME remove pickles after test run on VPS box
        pd.to_pickle(tids, '/app/shared/discogs_training_samples_series.pkl')

        tids_df = pd.DataFrame(tids.values.tolist(), index=tids.index)
        log_dataframe_info(
            LOGGER, tids_df, f"DataFrame with 1 target ID per column")
        pd.to_pickle(tids_df, '/app/shared/discogs_training_samples_df.pkl')

        index = pd.MultiIndex.from_frame(
            tids_df, names=[constants.QID, constants.TID])
        LOGGER.debug('Built candidate target IDs index: %s', index)
        pd.to_pickle(
            tids, '/app/shared/discogs_training_samples_multiindex.pkl')

        return index

    def _run_full_text_query(self, queries):
        tids = set()
        # FIXME butta le prossime righe nel cesso una volta ricostruito il DF preprocessato
        query = set()
        if isinstance(queries, str):
            query.add(queries)
        else:
            for q in queries:
                query.update(set(q.split()))
        #
        LOGGER.debug("Full-text query terms: %s", query)
        for result in tokens_fulltext_search(self.target_db_entity, self.boolean_mode, query):
            tids.add(result.catalog_id)
            LOGGER.debug('%s result: %s',
                         self.target_db_entity.__name__, result)

        if not tids:
            LOGGER.info('No target candidates for source query: %s', query)
            return nan

        top = list(tids)[:self.limit]
        LOGGER.debug('Top %d target candidates: %s', self.limit, top)
        return top
