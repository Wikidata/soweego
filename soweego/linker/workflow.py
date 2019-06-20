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

from soweego.commons import (
    constants,
    data_gathering,
    keys,
    target_database,
    text_utils,
    url_utils,
)
from soweego.commons.logging import log_dataframe_info
from soweego.linker import classifiers, neural_networks
from soweego.linker.feature_extraction import (
    CompareTokens,
    DateCompare,
    ExactList,
    OccupationQidSet,
    SimilarTokens,
    StringList,
)
from soweego.wikidata import api_requests, vocabulary

LOGGER = logging.getLogger(__name__)


def build_wikidata(goal: str, catalog: str, entity: str, dir_io: str) -> JsonReader:
    if goal == 'training':
        wd_io_path = os.path.join(
            dir_io, constants.WD_TRAINING_SET % (catalog, entity)
        )
        qids_and_tids = {}
    elif goal == 'classification':
        wd_io_path = os.path.join(
            dir_io, constants.WD_CLASSIFICATION_SET % (catalog, entity)
        )
        qids_and_tids = None
    else:
        raise ValueError(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'"
            % goal
        )

    catalog_pid = target_database.get_catalog_pid(catalog, entity)

    if os.path.exists(wd_io_path):
        LOGGER.info(
            "Will reuse existing Wikidata %s set: '%s'", goal, wd_io_path
        )
        if goal == 'training':
            _reconstruct_qids_and_tids(wd_io_path, qids_and_tids)

    else:
        LOGGER.info(
            "Building Wikidata %s set, output file '%s' ...", goal, wd_io_path
        )

        if goal == 'training':
            data_gathering.gather_target_ids(
                entity, catalog, catalog_pid, qids_and_tids
            )
            qids = qids_and_tids.keys()
        elif goal == 'classification':
            qids = data_gathering.gather_qids(entity, catalog, catalog_pid)

        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        os.makedirs(os.path.dirname(wd_io_path), exist_ok=True)
        with gzip.open(wd_io_path, 'wt') as wd_io:
            api_requests.get_data_for_linker(
                catalog,
                entity,
                qids,
                url_pids,
                ext_id_pids_to_urls,
                wd_io,
                qids_and_tids,
            )

    wd_df_reader = pd.read_json(wd_io_path, lines=True, chunksize=1000)

    LOGGER.info('Wikidata %s set built', goal)
    return wd_df_reader


def _reconstruct_qids_and_tids(wd_io_path, qids_and_tids):
    with gzip.open(wd_io_path, 'rt') as wd_io:
        for line in wd_io:
            item = json.loads(line.rstrip())
            qids_and_tids[item[keys.QID]] = {keys.TID: item[keys.TID]}
    LOGGER.debug(
        "Reconstructed dictionary with QIDS and target IDs from '%s'",
        wd_io_path,
    )


def build_target(goal, catalog, entity, qids_and_tids):
    handle_goal(goal)

    LOGGER.info('Building %s %s set ...', catalog, goal)

    if goal == 'classification':
        if qids_and_tids:
            raise ValueError(
                "Invalid 'qids_and_tids' parameter: it should be None when 'goal' is 'classification'"
            )
        qids_and_tids = {}
        data_gathering.gather_target_ids(
            entity,
            catalog,
            target_database.get_catalog_pid(catalog, entity),
            qids_and_tids,
        )

    target_df_reader = data_gathering.gather_target_dataset(
        goal, entity, catalog, _get_tids(qids_and_tids)
    )

    LOGGER.info('Target %s set built', goal)

    return target_df_reader


def _get_tids(qids_and_tids):
    tids = set()
    for data in qids_and_tids.values():
        for identifier in data[keys.TID]:
            tids.add(identifier)
    return tids


def preprocess(
        goal: str, wikidata_reader: JsonReader, target_reader: JsonReader
) -> Tuple[
    Generator[pd.DataFrame, None, None],
    Generator[pd.DataFrame, None, None]
]:
    handle_goal(goal)
    return (
        preprocess_wikidata(goal, wikidata_reader),
        preprocess_target(goal, target_reader),
    )


def extract_features(
        candidate_pairs: pd.MultiIndex,
        wikidata: pd.DataFrame,
        target: pd.DataFrame,
        path_io: str,
) -> pd.DataFrame:
    LOGGER.info('Extracting features ...')

    if os.path.exists(path_io):
        LOGGER.info("Will reuse existing features: '%s'", path_io)
        return pd.read_pickle(path_io)

    def in_both_datasets(col: str) -> bool:
        """Checks if `col` is available in both datasets"""
        return (col in wikidata.columns) and (col in target.columns)

    compare = rl.Compare(n_jobs=cpu_count())
    # TODO feature engineering on more fields
    # Feature 1: exact match on names
    if in_both_datasets(keys.NAME):
        compare.add(ExactList(keys.NAME, keys.NAME, label='name_exact'))

    if in_both_datasets(keys.URL):
        # Feature 2: exact match on URLs
        compare.add(ExactList(keys.URL, keys.URL, label='url_exact'))

        # Feature 3: match on URL tokens
        compare.add(
            CompareTokens(
                keys.URL_TOKENS,
                keys.URL_TOKENS,
                label='url_tokens',
                stopwords=text_utils.STOPWORDS_URL_TOKENS,
            )
        )

    # Feature 4: dates
    if in_both_datasets(keys.DATE_OF_BIRTH):
        compare.add(
            DateCompare(
                keys.DATE_OF_BIRTH, keys.DATE_OF_BIRTH, label='date_of_birth'
            )
        )
    if in_both_datasets(keys.DATE_OF_DEATH):
        compare.add(
            DateCompare(
                keys.DATE_OF_DEATH, keys.DATE_OF_DEATH, label='date_of_death'
            )
        )

    # Feature 5: Levenshtein distance on name tokens
    if in_both_datasets(keys.NAME_TOKENS):
        compare.add(
            StringList(
                keys.NAME_TOKENS, keys.NAME_TOKENS, label='name_levenshtein'
            )
        )

        # Feature 5: string kernel similarity on name tokens
        compare.add(
            StringList(
                keys.NAME_TOKENS,
                keys.NAME_TOKENS,
                algorithm='cosine',
                analyzer='char_wb',
                label='name_string_kernel_cosine',
            )
        )

        # Feature 6: similar name tokens
        compare.add(
            SimilarTokens(
                keys.NAME_TOKENS, keys.NAME_TOKENS, label='similar_name_tokens'
            )
        )

    # Feature 8: cosine similarity on descriptions
    if in_both_datasets(keys.DESCRIPTION):
        compare.add(
            StringList(
                keys.DESCRIPTION,
                keys.DESCRIPTION,
                algorithm='cosine',
                analyzer='soweego',
                label='description_cosine',
            )
        )

    # Feature 9: occupation QIDs
    occupations_col_name = vocabulary.LINKER_PIDS[vocabulary.OCCUPATION]
    if in_both_datasets(occupations_col_name):
        compare.add(
            OccupationQidSet(
                occupations_col_name,
                occupations_col_name,
                label='occupation_qids',
            )
        )

    # Feature 10: genre similar tokens
    if in_both_datasets(keys.GENRES):
        # Feature 9: genre similar tokens
        compare.add(
            SimilarTokens(
                keys.GENRES, keys.GENRES, label='genre_similar_tokens'
            )
        )

    # calculate feature vectors
    feature_vectors = compare.compute(candidate_pairs, wikidata, target)

    # drop duplicate FV
    feature_vectors = feature_vectors[~feature_vectors.index.duplicated()]

    os.makedirs(os.path.dirname(path_io), exist_ok=True)
    pd.to_pickle(feature_vectors, path_io)

    LOGGER.info("Features dumped to '%s'", path_io)
    LOGGER.info('Feature extraction done')
    return feature_vectors


def init_model(classifier, *args, **kwargs):
    if classifier is keys.NAIVE_BAYES:
        model = rl.NaiveBayesClassifier(**kwargs)

    elif classifier is keys.LINEAR_SVM:
        model = rl.SVMClassifier(**kwargs)

    elif classifier is keys.SVM:
        model = classifiers.SVCClassifier(**kwargs)

    elif classifier is keys.SINGLE_LAYER_PERCEPTRON:
        model = neural_networks.SingleLayerPerceptron(*args, **kwargs)

    elif classifier is keys.MULTI_LAYER_PERCEPTRON:
        model = neural_networks.MultiLayerPerceptron(*args, **kwargs)

    else:
        err_msg = f"""Unsupported classifier: {classifier}. It should be one of
                    {set(constants.CLASSIFIERS)}"""
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)

    return model


def preprocess_wikidata(goal: str, wikidata_reader: JsonReader) -> Generator[pd.DataFrame, None, None]:
    handle_goal(goal)

    LOGGER.info('Preprocessing Wikidata ...')

    for i, chunk in enumerate(wikidata_reader, 1):
        # 1. QID as index
        chunk.set_index(keys.QID, inplace=True)
        log_dataframe_info(
            LOGGER, chunk, f"Built index from '{keys.QID}' column"
        )

        # 2. Drop columns with null values only
        _drop_null_columns(chunk)

        # 3. Training only: join target IDs if multiple
        # TODO don't wipe out QIDs with > 1 positive samples!
        if goal == 'training':
            chunk[keys.TID] = chunk[keys.TID].map(
                lambda cell: cell[0] if isinstance(cell, list) else cell
            )

        # 4. Tokenize names
        for column in constants.NAME_FIELDS:
            if chunk.get(column) is not None:
                chunk[f'{column}_tokens'] = chunk[column].apply(
                    tokenize_values, args=(text_utils.tokenize,)
                )

        # 4b. Tokenize genres if available
        if chunk.get(keys.GENRES) is not None:
            chunk[keys.GENRES] = chunk[keys.GENRES].apply(
                tokenize_values, args=(text_utils.tokenize,)
            )

        # 5. Tokenize URLs
        chunk[keys.URL_TOKENS] = chunk[keys.URL].apply(
            tokenize_values, args=(url_utils.tokenize,)
        )

        # 6. Shared preprocessing
        chunk = _shared_preprocessing(
            chunk,
            _will_handle_birth_date(chunk),
            _will_handle_death_date(chunk),
        )

        LOGGER.info('Chunk %d done', i)
        yield chunk


def preprocess_target(goal: str, target_reader: pd.DataFrame) -> pd.DataFrame:
    handle_goal(goal)

    LOGGER.info('Preprocessing target ...')

    target = pd.concat([chunk for chunk in target_reader], sort=False)

    # 1. Drop target DB internal ID columns
    LOGGER.info("Dropping '%s' columns ...", keys.INTERNAL_ID)
    target.drop(columns=keys.INTERNAL_ID, inplace=True)
    log_dataframe_info(LOGGER, target, f"Dropped '{keys.INTERNAL_ID}'' columns")

    # 2. Rename non-null catalog ID column & drop others
    LOGGER.info(
        "Renaming '%s' column with no null values to '%s' & dropping '%s' columns with null values ...",
        keys.CATALOG_ID,
        keys.TID,
        keys.CATALOG_ID,
    )
    # If 'catalog_id' is one column (i.e., a Series),
    # then it won't have None values
    if isinstance(target[keys.CATALOG_ID], pd.Series):
        target[keys.TID] = target[keys.CATALOG_ID]
    else:
        no_nulls = target[keys.CATALOG_ID].dropna(axis=1)
        # It may happen that more than 1 column has no null values:
        # in this case, they must be identical,
        # so take the first one
        target[keys.TID] = (
            no_nulls.iloc[:, 0]
            if isinstance(no_nulls, pd.DataFrame)
            else no_nulls
        )
    target.drop(columns=keys.CATALOG_ID, inplace=True)
    log_dataframe_info(
        LOGGER,
        target,
        f"Renamed '{keys.CATALOG_ID}' column with no null values to '{keys.TID}' & dropped '{keys.CATALOG_ID}' columns with null values",
    )

    # 3. Drop columns with null values only
    LOGGER.info('Dropping columns with null values only ...')
    _drop_null_columns(target)

    # 4. Pair dates with their precision & drop precision columns
    if _will_handle_birth_date(target):
        LOGGER.info('Pairing birth date columns with precision ones ...')
        target[keys.DATE_OF_BIRTH] = list(
            zip(target[keys.DATE_OF_BIRTH], target[keys.BIRTH_PRECISION])
        )
        target.drop(columns=[keys.BIRTH_PRECISION], inplace=True)
        log_dataframe_info(
            LOGGER, target, 'Paired birth date columns with precision ones'
        )

    if _will_handle_death_date(target):
        LOGGER.info('Pairing death date columns with precision ones ...')
        target[keys.DATE_OF_DEATH] = list(
            zip(target[keys.DATE_OF_DEATH], target[keys.DEATH_PRECISION])
        )
        target.drop(columns=[keys.DEATH_PRECISION], inplace=True)

        log_dataframe_info(
            LOGGER, target, 'Paired death date columns with precision ones'
        )

    # 5. Aggregate denormalized data on target ID
    # TODO Token lists may contain duplicate tokens
    LOGGER.info("Aggregating denormalized data on '%s' column ...", keys.TID)
    target = target.groupby(keys.TID).agg(lambda x: list(set(x)))
    log_dataframe_info(
        LOGGER, target, f"Data indexed and aggregated on '{keys.TID}' column"
    )
    # 6. Shared preprocessing
    target = _shared_preprocessing(
        target, _will_handle_birth_date(target), _will_handle_death_date(target)
    )

    LOGGER.info('Target preprocessing done')
    return target


def _shared_preprocessing(df: pd.DataFrame, will_handle_birth_date: bool, will_handle_death_date: bool) -> pd.DataFrame:
    LOGGER.info('Normalizing fields with names ...')
    for column in constants.NAME_FIELDS:
        if df.get(column) is not None:
            df[column] = df[column].map(_normalize_values)

    _occupations_to_set(df)

    if will_handle_birth_date:
        LOGGER.info('Handling birth dates ...')
        _handle_dates(df, keys.DATE_OF_BIRTH)

    if will_handle_death_date:
        LOGGER.info('Handling death dates ...')
        _handle_dates(df, keys.DATE_OF_DEATH)

    return df


def _normalize_values(values):
    normalized_values = set()
    if values is nan or not any(values):
        return nan
    for value in values:
        if not value:
            continue
        _, normalized = text_utils.normalize(value)
        normalized_values.add(normalized)
    return list(normalized_values) if normalized_values else nan


def _drop_null_columns(target):
    target.dropna(axis=1, how='all', inplace=True)
    log_dataframe_info(LOGGER, target, 'Dropped columns with null values only')


def _handle_dates(df, column):
    # Datasets are hitting pandas timestamp limitations, see
    # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timestamp-limitations
    # Parse into Period instead, see
    # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-oob
    if df.get(column) is None:
        LOGGER.warning(
            "No '%s' column in DataFrame, won't handle its dates. Perhaps it was dropped because it contained null values only",
            column,
        )

    df[column] = df[column].map(_parse_dates_list, na_action='ignore')

    log_dataframe_info(LOGGER, df, 'Parsed dates')


def _will_handle_dates(df: pd.DataFrame) -> bool:
    return _will_handle_birth_date(df) and _will_handle_death_date(df)


def _will_handle_birth_date(df: pd.DataFrame) -> bool:
    dob_column = df.get(keys.DATE_OF_BIRTH)
    if dob_column is None:
        LOGGER.warning(
            "'%s' column is not in DataFrame, won't handle birth dates. Perhaps it was dropped because they contained null values only",
            keys.DATE_OF_BIRTH,
        )
        return False
    return True


def _will_handle_death_date(df: pd.DataFrame) -> bool:
    dod_column = df.get(keys.DATE_OF_DEATH)
    if dod_column is None:
        LOGGER.warning(
            "'%s' column is not in DataFrame, won't handle death dates. Perhaps it was dropped because they contained null values only",
            keys.DATE_OF_DEATH,
        )
        return False
    return True


def _parse_dates_list(dates_list):
    dates = []
    # 1990-11-06T00:00:00Z
    for date, precision in dates_list:
        if pd.isna(date) or pd.isna(precision):
            LOGGER.debug(
                'Skipping null value. Date: %s - Precision: %s', date, precision
            )
            continue
        if precision in vocabulary.DATE_PRECISION:
            if precision < vocabulary.YEAR:  # From decades to billion years
                LOGGER.debug(
                    'Date precision: %s. Falling back to YEAR, due to lack of support in Python pandas.Period',
                    vocabulary.DATE_PRECISION[precision],
                )
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
                'Unexpected date precision: %s. Will try to parse the date anyway',
                precision,
            )
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
            "Skipping date that can't be parsed: %s. Reason: %s", value, ve
        )


def handle_goal(goal):
    if goal not in ('training', 'classification'):
        raise ValueError(
            "Invalid 'goal' parameter: %s. Should be 'training' or 'classification'"
            % goal
        )


def _occupations_to_set(df):
    col_name = vocabulary.LINKER_PIDS[vocabulary.OCCUPATION]

    if col_name not in df.columns:
        LOGGER.info("No '%s' column in DataFrame, won't handle them", col_name)
        return

    def to_set(itm):
        # if it is an empty array (from source), or an
        # empty string (from target)
        if not itm:
            return set()

        # when coming from the DB, the occupations for target
        # are an array with only one element which is a string
        # of space separated occupations (or an empty string
        # in case there are no occupations)
        if len(itm) == 1:
            # get inner occupation ids and remove empty occupations
            itm = [x for x in itm[0].split() if x]

        return set(itm)

    LOGGER.info('Converting list of occupations into set ...')
    df[col_name] = df[col_name].apply(to_set)


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
