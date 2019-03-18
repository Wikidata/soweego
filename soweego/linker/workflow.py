#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Record linkage workflow, shared between training and classification."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import datetime
import gzip
import json
import logging
import os
from multiprocessing import cpu_count
from typing import Generator, Tuple

import pandas as pd
import recordlinkage as rl
from numpy import nan
from pandas.io.json.json import JsonReader

from soweego.commons import (constants, data_gathering, target_database,
                             text_utils, url_utils)
from soweego.commons.logging import log_dataframe_info
from soweego.linker.feature_extraction import StringList, UrlList, DateCompare, SimilarTokens
from soweego.wikidata import api_requests, vocabulary

LOGGER = logging.getLogger(__name__)


def build_wikidata(goal, catalog, entity, dir_io):
    if goal == 'training':
        wd_io_path = os.path.join(
            dir_io, constants.WD_TRAINING_SET % (catalog, entity))
        qids_and_tids = {}
    elif goal == 'classification':
        wd_io_path = os.path.join(
            dir_io, constants.WD_CLASSIFICATION_SET % (catalog, entity))
        qids_and_tids = None
    else:
        raise ValueError(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'" % goal)

    catalog_pid = target_database.get_pid(catalog)

    if os.path.exists(wd_io_path):
        LOGGER.info(
            "Will reuse existing Wikidata %s set: '%s'", goal, wd_io_path)
        if goal == 'training':
            with gzip.open(wd_io_path, 'rt') as wd_io:
                for line in wd_io:
                    item = json.loads(line.rstrip())
                    qids_and_tids[item[constants.QID]] = {
                        constants.TID: item[constants.TID]}
            LOGGER.debug(
                "Reconstructed dictionary with QIDS and target IDs from '%s'", wd_io_path)

    else:
        LOGGER.info(
            "Building Wikidata %s set, output file '%s' ...", goal, wd_io_path)

        if goal == 'training':
            data_gathering.gather_target_ids(
                entity, catalog, catalog_pid, qids_and_tids)
            qids = qids_and_tids.keys()
        elif goal == 'classification':
            qids = data_gathering.gather_qids(entity, catalog, catalog_pid)

        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            api_requests.get_data_for_linker(catalog,
                qids, url_pids, ext_id_pids_to_urls, wd_io, qids_and_tids)

    wd_df_reader = pd.read_json(wd_io_path, lines=True, chunksize=1000)

    LOGGER.info('Wikidata %s set built', goal)
    return wd_df_reader


def build_target(goal, catalog, entity, qids_and_tids):
    handle_goal(goal)

    LOGGER.info('Building %s %s set ...', catalog, goal)

    if goal == 'classification':
        if qids_and_tids:
            raise ValueError(
                "Invalid 'qids_and_tids' parameter: it should be None when 'goal' is 'classification'")
        qids_and_tids = {}
        data_gathering.gather_target_ids(
            entity, catalog, target_database.get_pid(catalog), qids_and_tids)

    target_df_reader = data_gathering.gather_target_dataset(
        goal, entity, catalog, _get_tids(qids_and_tids))

    LOGGER.info('Target %s set built', goal)

    return target_df_reader


def _get_tids(qids_and_tids):
    tids = set()
    for data in qids_and_tids.values():
        for identifier in data[constants.TID]:
            tids.add(identifier)
    return tids


def train_test_build(catalog, entity, dir_io):
    LOGGER.info("Building %s %s dataset for training and test, I/O directory: '%s'",
                catalog, entity, dir_io)

    # Wikidata
    wd_df_reader = build_wikidata('training', catalog, entity, dir_io)

    # Target
    target_df_reader = build_target('training', catalog, entity, qids_and_tids)

    return wd_df_reader, target_df_reader


def preprocess(goal: str, wikidata_reader: JsonReader, target_reader: JsonReader) -> Tuple[
    Generator[pd.DataFrame, None, None], Generator[pd.DataFrame, None, None]]:
    handle_goal(goal)
    return preprocess_wikidata(goal, wikidata_reader), preprocess_target(goal, target_reader)


def extract_features(candidate_pairs: pd.MultiIndex, wikidata: pd.DataFrame, target: pd.DataFrame,
                     path_io: str) -> pd.DataFrame:
    LOGGER.info('Extracting features ...')

    if os.path.exists(path_io):
        LOGGER.info("Will reuse existing features: '%s'", path_io)
        return pd.read_pickle(path_io)

    def in_both_datasets(col: str) -> bool:
        """Checks if `col` is available in both datasets"""
        return (col in wikidata.columns) and (col in target.columns)

    compare = rl.Compare(n_jobs=cpu_count())
    # TODO similar name match as a feature
    # TODO feature engineering on more fields
    # wikidata columns = Index(['tid', 'label', 'alias', 'description', 'url', 'given_name',
    #    'date_of_birth', 'date_of_death', 'place_of_death', 'birth_name',
    #    'place_of_birth', 'sex_or_gender', 'family_name', 'pseudonym'],
    # discogs columns = Index(['description_tokens', 'name_tokens', 'description', 'url', 'url_tokens',
    #    'name', 'born', 'born_precision', 'real_name', 'is_wiki',
    #    'data_quality', 'died', 'died_precision', 'identifier'],

    # Feature 1: exact match on URLs
    if in_both_datasets(constants.URL):
        compare.add(UrlList(constants.URL, constants.URL, label='url_exact'))

    # Feature 2: dates
    if in_both_datasets(constants.DATE_OF_BIRTH):
        compare.add(DateCompare(constants.DATE_OF_BIRTH,
                                constants.DATE_OF_BIRTH, label='date_of_birth'))

    if in_both_datasets(constants.DATE_OF_DEATH):
        compare.add(DateCompare(constants.DATE_OF_DEATH,
                                constants.DATE_OF_DEATH, label='date_of_death'))

    # Feature 3: Levenshtein distance on names and similar tokens
    if in_both_datasets(constants.NAME_TOKENS):
        compare.add(StringList(constants.NAME_TOKENS,
                               constants.NAME_TOKENS, label='name_levenshtein'))
        compare.add(SimilarTokens(constants.NAME_TOKENS, constants.NAME_TOKENS, label='similar_name_tokens'))

    # Feature 4: cosine similarity on descriptions
    if in_both_datasets(constants.DESCRIPTION):
        compare.add(StringList(constants.DESCRIPTION, constants.DESCRIPTION,
                               algorithm='cosine', analyzer='soweego', label='description_cosine'))

    feature_vectors = compare.compute(candidate_pairs, wikidata, target)
    pd.to_pickle(feature_vectors, path_io)

    LOGGER.info("Features dumped to '%s'", path_io)
    LOGGER.info('Feature extraction done')
    return feature_vectors


def init_model(classifier, binarize):
    # TODO expose other useful parameters
    if classifier is rl.NaiveBayesClassifier:
        model = classifier(binarize=binarize)
    elif classifier is rl.SVMClassifier:
        # TODO implement SVM
        raise NotImplementedError
    return model


def preprocess_wikidata(goal, wikidata_reader):
    handle_goal(goal)

    LOGGER.info('Preprocessing Wikidata ...')

    for i, chunk in enumerate(wikidata_reader, 1):
        # 1. QID as index
        chunk.set_index(constants.QID, inplace=True)
        log_dataframe_info(
            LOGGER, chunk, f"Built index from '{constants.QID}' column")

        # 2. Drop columns with null values only
        _drop_null_columns(chunk)

        # 3. Training only: join target IDs if multiple
        # TODO don't wipe out QIDs with > 1 positive samples!
        if goal == 'training':
            chunk[constants.TID] = chunk[constants.TID].map(
                lambda cell: cell[0] if isinstance(cell, list) else cell)

        # 4. Tokenize & join strings lists columns
        for column in (constants.NAME, constants.PSEUDONYM):
            chunk[f'{column}_tokens'] = chunk[column].apply(
                tokenize_values, args=(text_utils.tokenize,))

        # 5. Tokenize & join URLs lists
        chunk[constants.URL_TOKENS] = chunk[constants.URL].apply(
            tokenize_values, args=(url_utils.tokenize,))

        # 6. Shared preprocessing
        chunk = _shared_preprocessing(chunk, _will_handle_dates(chunk))

        LOGGER.info('Chunk %d done', i)
        yield chunk

    LOGGER.info('Wikidata preprocessing done')


def preprocess_target(goal, target_reader):
    handle_goal(goal)

    LOGGER.info('Preprocessing target ...')

    target = pd.concat([chunk for chunk in target_reader], sort=False)

    # 1. Drop target DB internal ID columns
    LOGGER.info("Dropping '%s' columns ...", constants.INTERNAL_ID)
    target.drop(columns=constants.INTERNAL_ID, inplace=True)
    log_dataframe_info(
        LOGGER, target, f"Dropped '{constants.INTERNAL_ID}'' columns")

    # 2. Rename non-null catalog ID column & drop others
    LOGGER.info("Renaming '%s' column with no null values to '%s' & dropping '%s' columns with null values ...",
                constants.CATALOG_ID, constants.TID, constants.CATALOG_ID)
    # If 'catalog_id' is one column (i.e., a Series),
    # then it won't have None values
    if isinstance(target[constants.CATALOG_ID], pd.Series):
        target[constants.TID] = target[constants.CATALOG_ID]
    else:
        no_nulls = target[constants.CATALOG_ID].dropna(axis=1)
        # It may happen that more than 1 column has no null values:
        # in this case, they must be identical,
        # so take the first one
        target[constants.TID] = no_nulls.iloc[:, 0] if isinstance(
            no_nulls, pd.DataFrame) else no_nulls
    target.drop(columns=constants.CATALOG_ID, inplace=True)
    log_dataframe_info(
        LOGGER, target,
        f"Renamed '{constants.CATALOG_ID}' column with no null values to '{constants.TID}' & dropped '{constants.CATALOG_ID}' columns with null values")

    # 3. Drop columns with null values only
    LOGGER.info('Dropping columns with null values only ...')
    _drop_null_columns(target)

    will_handle_dates = _will_handle_dates(target)

    # 4. Pair dates with their precision & drop precision columns
    if will_handle_dates:
        LOGGER.info('Pairing date columns with precision ones ...')
        target[constants.DATE_OF_BIRTH] = list(
            zip(target[constants.DATE_OF_BIRTH], target[constants.BIRTH_PRECISION]))
        target[constants.DATE_OF_DEATH] = list(
            zip(target[constants.DATE_OF_DEATH], target[constants.DEATH_PRECISION]))
        target.drop(columns=[constants.BIRTH_PRECISION,
                             constants.DEATH_PRECISION], inplace=True)
        log_dataframe_info(
            LOGGER, target, 'Paired date columns with precision ones')

    # 5. Aggregate denormalized data on target ID
    # TODO Token lists may contain duplicate tokens
    LOGGER.info("Aggregating denormalized data on '%s' column ...",
                constants.TID)
    target = target.groupby(constants.TID).agg(lambda x: list(set(x)))
    log_dataframe_info(
        LOGGER, target, f"Data indexed and aggregated on '{constants.TID}' column")

    # 6. Shared preprocessing
    target = _shared_preprocessing(target, will_handle_dates)

    LOGGER.info('Target preprocessing done')
    return target


def _shared_preprocessing(df, will_handle_dates):
    LOGGER.info('Joining descriptions ...')
    _join_descriptions(df)

    if will_handle_dates:
        LOGGER.info('Handling dates ...')
        _handle_dates(df)

    LOGGER.info('Stringifying lists with a single value ...')
    df = _pull_out_from_single_value_list(df)

    return df


def _drop_null_columns(target):
    target.dropna(axis=1, how='all', inplace=True)
    log_dataframe_info(
        LOGGER, target, 'Dropped columns with null values only')


def _handle_dates(df):
    # Datasets are hitting pandas timestamp limitations, see
    # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timestamp-limitations
    # Parse into Period instead, see
    # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-oob
    for column in (constants.DATE_OF_BIRTH, constants.DATE_OF_DEATH):
        if df.get(column) is None:
            LOGGER.warning(
                "No '%s' column in DataFrame, won't handle its dates. Perhaps it was dropped because it contained null values only",
                column)
            continue

        df[column] = df[column].map(
            _parse_dates_list, na_action='ignore')

    log_dataframe_info(LOGGER, df, 'Parsed dates')


def _will_handle_dates(df):
    dob_column = df.get(constants.DATE_OF_BIRTH)
    dod_column = df.get(constants.DATE_OF_DEATH)

    if dob_column is None and dod_column is None:
        LOGGER.warning(
            "Neither '%s' nor '%s' column in DataFrame, won't handle dates. Perhaps they were dropped because they contained null values only",
            constants.DATE_OF_BIRTH, constants.DATE_OF_DEATH)
        return False

    return True


def _parse_dates_list(dates_list):
    dates = []
    # 1990-11-06T00:00:00Z
    for date, precision in dates_list:
        if pd.isna(date) or pd.isna(precision):
            LOGGER.debug(
                'Skipping null value. Date: %s - Precision: %s', date, precision)
            continue
        if precision in vocabulary.DATE_PRECISION:
            if precision < vocabulary.YEAR:  # From decades to billion years
                LOGGER.debug('Date precision: %s. Falling back to YEAR, due to lack of support in Python pandas.Period',
                             vocabulary.DATE_PRECISION[precision])
                _build_date_object(date, 4, dates)
            elif precision == vocabulary.YEAR:
                _build_date_object(date, 4, dates)
            elif precision == vocabulary.MONTH:
                _build_date_object(date, 7, dates)
            elif precision == vocabulary.DAY:
                _build_date_object(date, 10, dates)
            elif precision == vocabulary.HOUR:
                _build_date_object(date, 13, dates)
            elif precision == vocabulary.MINUTE:
                _build_date_object(date, 16, dates)
            elif precision == vocabulary.SECOND:
                _build_date_object(date, len(date), dates)
        else:
            LOGGER.warning(
                'Unexpected date precision: %s. Will try to parse the date anyway', precision)
            _build_date_object(date, len(date), dates)
    if not dates:
        return pd.NaT
    return dates


def _build_date_object(value, slice_index, to_dates_list):
    if isinstance(value, (datetime.date, datetime.datetime)):
        value = value.isoformat()

    try:
        to_dates_list.append(pd.Period(value[:slice_index]))
    except ValueError as ve:
        LOGGER.warning(
            "Skipping date that can't be parsed: %s. Reason: %s", value, ve)


def handle_goal(goal):
    if goal not in ('training', 'classification'):
        raise ValueError(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'" % goal)


def _pull_out_from_single_value_list(df):
    # TODO this produces columns with either strings or lists, probably not ideal
    df = df.applymap(lambda cell: cell[0] if isinstance(
        cell, list) and len(cell) == 1 else cell)
    log_dataframe_info(LOGGER, df, 'Stringified lists with a single value')
    return df


def _join_descriptions(df):
    # TODO It certainly doesn't make sense to compare descriptions in different languages
    column = df.get(constants.DESCRIPTION)
    if column is None:
        LOGGER.warning(
            "No '%s' column in DataFrame, won't join values. Perhaps it was dropped because it contained null values only",
            constants.DESCRIPTION)
        return

    df[constants.DESCRIPTION] = df[constants.DESCRIPTION].str.join(' ')
    log_dataframe_info(LOGGER, df, 'Joined descriptions')


def tokenize_values(values, tokenize_func):
    if values is nan:
        return nan
    all_tokens = set()
    for value in values:
        value_tokens = tokenize_func(value)
        if value_tokens:
            all_tokens.update(value_tokens)
    if not all_tokens:
        LOGGER.debug('No tokens from list of values: %s', values)
        return nan
    return list(all_tokens)
