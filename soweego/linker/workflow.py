#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Record linkage workflow, shared between training and classification."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from io import StringIO
from typing import Tuple

import recordlinkage as rl
from pandas import DataFrame, MultiIndex, concat
from pandas.io.json.json import JsonReader

from soweego.commons import constants, text_utils, url_utils
from soweego.linker.feature_extraction import StringList, UrlList

LOGGER = logging.getLogger(__name__)


def preprocess(wikidata_reader: JsonReader, target_reader: JsonReader) -> Tuple[DataFrame, DataFrame]:
    return _preprocess_wikidata(wikidata_reader), _preprocess_target(target_reader)


def _preprocess_target(target_reader):
    LOGGER.info('Preprocessing target ...')

    # 1. Load into a DataFrame
    # Needed to avoid inconsistent aggregations
    # if we run step 2 on chunks
    # TODO Segfault when running in Docker container
    target = concat([chunk for chunk in target_reader],
                    ignore_index=True, sort=False)
    debug_buffer = StringIO()
    target.info(buf=debug_buffer)
    LOGGER.debug('Target loaded into a pandas DataFrame: %s',
                 debug_buffer.getvalue())

    # 2. Aggregate denormalized data on target ID
    target = target.groupby(constants.TID, as_index=False).agg(
        lambda x: list(set(x)))
    debug_buffer = StringIO()
    target.info(buf=debug_buffer)
    LOGGER.debug('Aggregated data on %s: %s',
                 constants.TID, debug_buffer.getvalue())

    # 3. Pull out the value from lists with a single value
    target = _pull_out_from_single_value_list(target)
    debug_buffer = StringIO()
    target.info(buf=debug_buffer)
    LOGGER.debug('Stringified lists with a single value: %s',
                 debug_buffer.getvalue())

    # 4. Join the list of descriptions
    _join_descriptions(target)
    debug_buffer = StringIO()
    target.info(buf=debug_buffer)
    LOGGER.debug('Joined descriptions: %s', debug_buffer.getvalue())

    LOGGER.info('Target preprocessing done')

    return target


def _preprocess_wikidata(wikidata_reader):
    wd_chunks = []
    LOGGER.info('Preprocessing Wikidata ...')

    for i, chunk in enumerate(wikidata_reader, 1):
        # 1. Pull out the value from lists with a single value
        chunk = _pull_out_from_single_value_list(chunk)

        # 2. Join target ids if multiple
        chunk[constants.TID] = chunk[constants.TID].map(
            lambda cell: ' '.join(cell) if isinstance(cell, list) else cell)

        # 3. Tokenize & join strings lists columns
        for column in (constants.LABEL, constants.PSEUDONYM):
            chunk['%s_tokens' % column] = chunk[column].map(
                _preprocess_strings_list, na_action='ignore')

        # 4. Tokenize & join URLs lists
        chunk['%s_tokens' % constants.URL] = chunk[constants.URL].map(
            _preprocess_urls_list, na_action='ignore')

        # 5. Join the list of descriptions
        _join_descriptions(chunk)

        LOGGER.info('Chunk %d done', i)

        wd_chunks.append(chunk)

    LOGGER.info('Wikidata preprocessing done')
    return concat(wd_chunks, ignore_index=True, sort=False)


def _pull_out_from_single_value_list(df):
    df = df.applymap(
        lambda cell: cell[0] if isinstance(cell, list) and len(cell) == 1 else cell)
    return df


def _join_descriptions(df):
    # TODO It certainly doesn't make sense to compare descriptions in different languages
    df[constants.DESCRIPTION] = df[constants.DESCRIPTION].map(
        ' '.join, na_action='ignore')


def _preprocess_strings_list(strings_list):
    joined = []
    for value in strings_list:
        tokens = text_utils.tokenize(value)
        if tokens:
            joined.append(' '.join(tokens))
    return joined


def _preprocess_urls_list(urls_list):
    joined = []
    for value in urls_list:
        tokens = url_utils.tokenize(value)
        if tokens:
            joined.append(' '.join(tokens))
    return joined


def extract_features(candidate_pairs: MultiIndex, wikidata: DataFrame, target: DataFrame) -> DataFrame:
    LOGGER.info('Extracting features ...')

    compare = rl.Compare()
    # TODO similar name match as a feature
    # TODO feature engineering on more fields
    # wikidata columns = Index(['tid', 'label', 'alias', 'description', 'url', 'given_name',
    #    'date_of_birth', 'date_of_death', 'place_of_death', 'birth_name',
    #    'place_of_birth', 'sex_or_gender', 'family_name', 'pseudonym'],
    # discogs columns = Index(['description_tokens', 'name_tokens', 'description', 'url', 'url_tokens',
    #    'name', 'born', 'born_precision', 'real_name', 'is_wiki',
    #    'data_quality', 'died', 'died_precision', 'identifier'],
    # Feature 1: exact match on URLs
    compare.add(UrlList('url', 'url', label='url_exact'))
    # Feature 2: dates
    # TODO parse dates
    # compare.date('date_of_birth', 'born', label='birth_date')
    # compare.date('date_of_death', 'died', label='death_date')
    # Feature 3: Levenshtein distance on names
    compare.add(StringList('label_tokens',
                           'name_tokens', label='name_levenshtein'))
    # Feture 4: cosine similarity on descriptions
    compare.add(StringList('description', 'description',
                           algorithm='cosine', analyzer='soweego', label='description_cosine'))
    feature_vectors = compare.compute(candidate_pairs, wikidata, target)

    LOGGER.info('Feature extraction done')
    return feature_vectors
