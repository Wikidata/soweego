#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""`Record linkage <https://en.wikipedia.org/wiki/Record_linkage>`_ workflow.
It is a pipeline composed of the following main steps:

1. build the Wikidata (:func:`build_wikidata`)
   and target (:func:`build_target`) datasets
2. preprocess both (:func:`preprocess_wikidata` and :func:`preprocess_target`)
3. extract features by comparing pairs of Wikidata and target values
   (:func:`extract_features`)

"""
import datetime
import gzip
import json
import logging
import os
from multiprocessing import cpu_count
from typing import Iterator, Set

import pandas as pd
import recordlinkage as rl
from numpy import nan
from pandas import read_sql
from pandas.io.json._json import JsonReader
from sqlalchemy.orm import Query

from soweego.commons import (
    constants,
    data_gathering,
    keys,
    target_database,
    text_utils,
    url_utils,
    utils,
)
from soweego.commons.db_manager import DBManager
from soweego.commons.logging import log_dataframe_info
from soweego.linker import features
from soweego.wikidata import api_requests, vocabulary

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

LOGGER = logging.getLogger(__name__)


def build_wikidata(
    goal: str, catalog: str, entity: str, dir_io: str
) -> JsonReader:
    """Build a Wikidata dataset for training or classification purposes:
    workflow step 1.

    Data is gathered from the
    `SPARQL endpoint <https://query.wikidata.org/>`_ and the
    `Web API <https://www.wikidata.org/w/api.php>`_.

    **How it works:**

    1. gather relevant Wikidata items that *hold* (for *training*)
       or *lack* (for *classification*) identifiers of the given catalog
    2. gather relevant item data
    3. dump the dataset to a gzipped `JSON Lines <http://jsonlines.org/>`_ file
    4. read the dataset into a generator of :class:`pandas.DataFrame` chunks
       for memory-efficient processing

    :param goal: ``{'training', 'classification'}``.
      Whether to build a dataset for training or classification
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param dir_io: input/output directory where working files
      will be read/written
    :return: the generator yielding :class:`pandas.DataFrame` chunks
    """
    qids_and_tids, wd_io_path = _handle_goal(goal, catalog, entity, dir_io)
    catalog_pid = target_database.get_catalog_pid(catalog, entity)

    if not os.path.isfile(wd_io_path):
        LOGGER.info(
            "Building Wikidata %s set for %s %s, output file '%s' ...",
            goal,
            catalog,
            entity,
            wd_io_path,
        )

        # Make working folders
        os.makedirs(os.path.dirname(wd_io_path), exist_ok=True)

        # 1. Gather Wikidata items
        if goal == 'training':
            # WITH target IDs
            data_gathering.gather_target_ids(
                entity, catalog, catalog_pid, qids_and_tids
            )
            qids = qids_and_tids.keys()

        elif goal == 'classification':
            # WITHOUT target IDs
            qids = data_gathering.gather_qids(entity, catalog, catalog_pid)

        # 2. Collect relevant data, and 3. dump to gzipped JSON Lines
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()

        with gzip.open(wd_io_path, 'wt') as wd_io:
            api_requests.get_data_for_linker(
                catalog,
                entity,
                qids,
                url_pids,
                ext_id_pids_to_urls,
                qids_and_tids,
                wd_io,
            )

    # Cached dataset, for development purposes
    else:
        LOGGER.info(
            "Will reuse existing Wikidata %s set: '%s'", goal, wd_io_path
        )
        if goal == 'training':
            _reconstruct_qids_and_tids(wd_io_path, qids_and_tids)

    LOGGER.info('Wikidata %s set built', goal)

    return pd.read_json(wd_io_path, lines=True, chunksize=1000)


def build_target(
    goal: str, catalog: str, entity: str, identifiers: Set[str]
) -> Iterator[pd.DataFrame]:
    """Build a target catalog dataset for training or classification purposes:
    workflow step 1.

    Data is gathered by querying the ``s51434__mixnmatch_large_catalogs_p``
    database. This is where the :mod:`importer` inserts processed catalog dumps.

    The database is located in
    `ToolsDB <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases>`_
    under the Wikimedia
    `Toolforge <https://wikitech.wikimedia.org/wiki/Portal:Toolforge>`_ infrastructure.
    See `how to connect <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connecting_to_the_database_replicas>`_.

    :param goal: ``{'training', 'classification'}``.
      Whether to build a dataset for training or classification
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param identifiers: a set of catalog IDs to gather data for
    :return: the generator yielding :class:`pandas.DataFrame` chunks
    """
    utils.check_goal_value(goal)

    LOGGER.info('Building target %s set for %s %s ...', goal, catalog, entity)

    # Target catalog ORM entities/DB tables
    base, link, nlp = (
        target_database.get_main_entity(catalog, entity),
        target_database.get_link_entity(catalog, entity),
        target_database.get_nlp_entity(catalog, entity),
    )
    tables = [table for table in (base, link, nlp) if table]

    # Initial query with all non-null tables
    query = Query(tables)
    # Remove `base` to avoid outer join with itself
    tables.remove(base)
    # Outer joins
    for table in tables:
        query = query.outerjoin(table, base.catalog_id == table.catalog_id)
    # Condition
    query = query.filter(base.catalog_id.in_(identifiers)).enable_eagerloads(
        False
    )

    sql = query.statement
    LOGGER.debug('SQL query to be fired: %s', sql)

    # Avoid loading query result in memory
    db_engine = DBManager().get_engine().execution_options(stream_results=True)

    return read_sql(sql, db_engine, chunksize=1000)


def preprocess_wikidata(
    goal: str, wikidata_reader: JsonReader
) -> Iterator[pd.DataFrame]:
    """Preprocess a Wikidata dataset: workflow step 2.

    This function consumes :class:`pandas.DataFrame` chunks and
    should be pipelined after :func:`build_wikidata`.

    **Preprocessing actions:**

    1. set QIDs as :class:`pandas.core.indexes.base.Index` of the chunk
    2. drop columns with null values only
    3. *(training)* ensure one target ID per QID
    4. tokenize names, URLs, genres, when applicable
    5. *(shared with* :func:`preprocess_target` *)*
       normalize columns with names, occupations, dates, when applicable

    :param goal: ``{'training', 'classification'}``.
      Whether the dataset is for training or classification
    :param wikidata_reader: a dataset reader as returned by
      :func:`build_wikidata`
    :return: the generator yielding preprocessed
      :class:`pandas.DataFrame` chunks
    """
    utils.check_goal_value(goal)

    LOGGER.info('Preprocessing Wikidata %s set ...', goal)

    for i, chunk in enumerate(wikidata_reader, 1):
        # 1. QID as index
        chunk.set_index(keys.QID, inplace=True)
        log_dataframe_info(
            LOGGER, chunk, f"Built index from '{keys.QID}' column"
        )

        # 2. Drop columns with null values only
        _drop_null_columns(chunk)

        # 3. Training only: ensure 1 target ID
        if goal == 'training':
            # This wipes out QIDs with > 1 positive samples,
            # but the impact can be neglected
            chunk[keys.TID] = chunk[keys.TID].map(
                lambda cell: cell[0] if isinstance(cell, list) else cell
            )

        # 4. Tokenize names
        for column in constants.NAME_FIELDS:
            if chunk.get(column) is not None:
                chunk[f'{column}_tokens'] = chunk[column].apply(
                    _tokenize_values, args=(text_utils.tokenize,)
                )

        # 4b. Tokenize genres if available
        if chunk.get(keys.GENRES) is not None:
            chunk[keys.GENRES] = chunk[keys.GENRES].apply(
                _tokenize_values, args=(text_utils.tokenize,)
            )

        # 5. Tokenize URLs
        chunk[keys.URL_TOKENS] = chunk[keys.URL].apply(
            _tokenize_values, args=(url_utils.tokenize,)
        )

        # 6. Shared preprocessing
        chunk = _shared_preprocessing(
            chunk,
            _will_handle_birth_dates(chunk),
            _will_handle_death_dates(chunk),
        )

        LOGGER.info('Chunk %d done', i)

        yield chunk


def preprocess_target(
    goal: str, target_reader: Iterator[pd.DataFrame]
) -> pd.DataFrame:
    """Preprocess a target catalog dataset: workflow step 2.

    This function consumes :class:`pandas.DataFrame` chunks and
    should be pipelined after :func:`build_target`.

    **Preprocessing actions:**

    1. drop unneeded columns holding target DB primary keys
    2. rename non-null catalog ID columns & drop others
    3. drop columns with null values only
    4. pair dates with their precision and drop precision columns
       when applicable
    5. aggregate denormalized data on target ID
    6. *(shared with* :func:`preprocess_wikidata` *)*
       normalize columns with names, occupations, dates, when applicable

    :param goal: ``{'training', 'classification'}``.
      Whether the dataset is for training or classification
    :param target_reader: a dataset reader as returned by
      :func:`build_target`
    :return: the generator yielding preprocessed
      :class:`pandas.DataFrame` chunks
    """
    utils.check_goal_value(goal)

    LOGGER.info('Preprocessing target ...')

    # Target data is denormalized, so we must consume the input generator
    # to perform consistent aggregations later
    target = pd.concat([chunk for chunk in target_reader], sort=False)

    # 1. Drop target DB internal ID columns
    LOGGER.info("Dropping '%s' columns ...", keys.INTERNAL_ID)
    target.drop(columns=keys.INTERNAL_ID, inplace=True)
    log_dataframe_info(LOGGER, target, f"Dropped '{keys.INTERNAL_ID}'' columns")

    # 2. Rename non-null catalog ID column & drop others
    _rename_or_drop_tid_columns(target)

    # 3. Drop columns with null values only
    LOGGER.info('Dropping columns with null values only ...')
    _drop_null_columns(target)

    # 4. Pair dates with their precision & drop precision columns
    _pair_dates(target)

    # 5. Aggregate denormalized data on target ID
    # TODO Token lists may contain duplicate tokens
    LOGGER.info("Aggregating denormalized data on '%s' column ...", keys.TID)
    target = target.groupby(keys.TID).agg(lambda x: list(set(x)))
    log_dataframe_info(
        LOGGER, target, f"Data indexed and aggregated on '{keys.TID}' column"
    )

    # 6. Shared preprocessing
    target = _shared_preprocessing(
        target,
        _will_handle_birth_dates(target),
        _will_handle_death_dates(target),
    )

    LOGGER.info('Target preprocessing done')

    return target


def extract_features(
    candidate_pairs: pd.MultiIndex,
    wikidata: pd.DataFrame,
    target: pd.DataFrame,
    path_io: str,
) -> pd.DataFrame:
    """Extract feature vectors by comparing pairs of
    *(Wikidata, target catalog)* records.

    **Main features:**

    - exact match on full names and URLs
    - match on tokenized names, URLs, and genres
    - `Levenshtein distance <https://en.wikipedia.org/wiki/Levenshtein_distance>`_
      on name tokens
    - `string kernel <https://en.wikipedia.org/wiki/String_kernel>`_
      similarity on name tokens
    - weighted intersection on name tokens
    - match on dates by maximum shared precision
    - `cosine similarity <https://en.wikipedia.org/wiki/Cosine_similarity>`_
      on textual descriptions
    - match on occupation QIDs

    See :mod:`features` for more details.

    This function uses multithreaded parallel processing.

    :param candidate_pairs: an index of *(QID, target ID)* pairs
      that should undergo comparison
    :param wikidata: a preprocessed Wikidata dataset (typically a chunk)
    :param target: a preprocessed target catalog dataset (typically a chunk)
    :param path_io: input/output path to an extracted feature file
    :return: the feature vectors dataset
    """
    LOGGER.info('Extracting features ...')

    # Early return cached features, for development purposes
    if os.path.isfile(path_io):
        LOGGER.info("Will reuse existing features: '%s'", path_io)
        return pd.read_pickle(path_io)

    def in_both_datasets(col: str) -> bool:
        return (col in wikidata.columns) and (col in target.columns)

    feature_extractor = rl.Compare(n_jobs=cpu_count())

    # Exact match on full name
    name_column = keys.NAME
    if in_both_datasets(name_column):
        feature_extractor.add(
            features.ExactMatch(
                name_column, name_column, label=f'{name_column}_exact'
            )
        )

    # URL features
    if in_both_datasets(keys.URL):
        _add_url_features(feature_extractor)

    # Date features
    _add_date_features(feature_extractor, in_both_datasets)

    # Name tokens features
    if in_both_datasets(keys.NAME_TOKENS):
        _add_name_tokens_features(feature_extractor)

    # Cosine similarity on description
    description_column = keys.DESCRIPTION
    if in_both_datasets(description_column):
        feature_extractor.add(
            features.SimilarStrings(
                description_column,
                description_column,
                algorithm='cosine',
                analyzer='soweego',
                label=f'{description_column}_cosine',
            )
        )

    # Match on occupation QIDs
    occupations_column = keys.OCCUPATIONS
    if in_both_datasets(occupations_column):
        feature_extractor.add(
            features.SharedOccupations(
                occupations_column,
                occupations_column,
                label=f'{occupations_column}_shared',
            )
        )

    # Match on tokenized genres
    genres_column = keys.GENRES
    if in_both_datasets(genres_column):
        feature_extractor.add(
            features.SharedTokens(
                genres_column,
                genres_column,
                label=f'{genres_column}_tokens_shared',
            )
        )

    feature_vectors = feature_extractor.compute(
        candidate_pairs, wikidata, target
    )
    feature_vectors = feature_vectors[
        ~feature_vectors.index.duplicated()  # Drop duplicates
    ]

    os.makedirs(os.path.dirname(path_io), exist_ok=True)
    pd.to_pickle(feature_vectors, path_io)
    LOGGER.info("Features dumped to '%s'", path_io)

    LOGGER.info('Feature extraction done')

    return feature_vectors


def _add_date_features(feature_extractor, in_both_datasets):
    birth_column, death_column = keys.DATE_OF_BIRTH, keys.DATE_OF_DEATH

    if in_both_datasets(birth_column):
        feature_extractor.add(
            features.SimilarDates(
                birth_column, birth_column, label=f'{birth_column}_similar'
            )
        )

    if in_both_datasets(death_column):
        feature_extractor.add(
            features.SimilarDates(
                death_column, death_column, label=f'{death_column}_similar'
            )
        )


def _add_url_features(feature_extractor):
    url_column, url_tokens_column = keys.URL, keys.URL_TOKENS

    # Exact match on URLs
    feature_extractor.add(
        features.ExactMatch(url_column, url_column, label=f'{url_column}_exact')
    )

    # Match on URL tokens
    feature_extractor.add(
        features.SharedTokensPlus(
            url_tokens_column,
            url_tokens_column,
            label=f'{url_tokens_column}_shared',
            stop_words=text_utils.STOPWORDS_URL_TOKENS,
        )
    )


def _add_name_tokens_features(feature_extractor):
    name_tokens_column = keys.NAME_TOKENS

    # Levenshtein distance on name tokens
    feature_extractor.add(
        features.SimilarStrings(
            name_tokens_column,
            name_tokens_column,
            label=f'{name_tokens_column}_levenshtein',
        )
    )

    # String kernel similarity on name tokens
    feature_extractor.add(
        features.SimilarStrings(
            name_tokens_column,
            name_tokens_column,
            algorithm='cosine',
            analyzer='char_wb',
            label=f'{name_tokens_column}_string_kernel_cosine',
        )
    )

    # Weighted intersection of name tokens
    feature_extractor.add(
        features.SharedTokens(
            name_tokens_column,
            name_tokens_column,
            label=f'{name_tokens_column}_shared',
        )
    )


def _pair_dates(target):
    if _will_handle_birth_dates(target):
        LOGGER.info('Pairing birth date columns with precision ones ...')

        target[keys.DATE_OF_BIRTH] = list(
            zip(target[keys.DATE_OF_BIRTH], target[keys.BIRTH_PRECISION])
        )
        target.drop(columns=[keys.BIRTH_PRECISION], inplace=True)

        log_dataframe_info(
            LOGGER, target, 'Paired birth date columns with precision ones'
        )

    if _will_handle_death_dates(target):
        LOGGER.info('Pairing death date columns with precision ones ...')

        target[keys.DATE_OF_DEATH] = list(
            zip(target[keys.DATE_OF_DEATH], target[keys.DEATH_PRECISION])
        )
        target.drop(columns=[keys.DEATH_PRECISION], inplace=True)

        log_dataframe_info(
            LOGGER, target, 'Paired death date columns with precision ones'
        )


def _rename_or_drop_tid_columns(target):
    LOGGER.info(
        "Renaming '%s' column with no null values to '%s' "
        "& dropping '%s' columns with null values ...",
        keys.CATALOG_ID,
        keys.TID,
        keys.CATALOG_ID,
    )

    # If `catalog_id` is one column (i.e., a `Series`),
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
        f"Renamed '{keys.CATALOG_ID}' column with no null values to "
        f"'{keys.TID}' & dropped '{keys.CATALOG_ID}' columns with null values",
    )


def _shared_preprocessing(
    df: pd.DataFrame, will_handle_birth_date: bool, will_handle_death_date: bool
) -> pd.DataFrame:
    LOGGER.info('Normalizing columns with names ...')
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


def _handle_goal(goal, catalog, entity, dir_io):
    if goal == 'training':
        wd_io_path = os.path.join(
            dir_io, constants.WD_TRAINING_SET.format(catalog, entity)
        )
        qids_and_tids = {}

    elif goal == 'classification':
        wd_io_path = os.path.join(
            dir_io, constants.WD_CLASSIFICATION_SET.format(catalog, entity)
        )
        qids_and_tids = None

    else:
        raise ValueError(
            f"Invalid 'goal' parameter: {goal}. "
            f"It should be 'training' or 'classification'"
        )
    return qids_and_tids, wd_io_path


def _reconstruct_qids_and_tids(wd_io_path, qids_and_tids):
    with gzip.open(wd_io_path, 'rt') as wd_io:
        for line in wd_io:
            item = json.loads(line.rstrip())
            qids_and_tids[item[keys.QID]] = {keys.TID: item[keys.TID]}
    LOGGER.debug(
        "Reconstructed dictionary with QIDS and target IDs from '%s'",
        wd_io_path,
    )


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


def _will_handle_birth_dates(df: pd.DataFrame) -> bool:
    dob_column = df.get(keys.DATE_OF_BIRTH)
    if dob_column is None:
        LOGGER.warning(
            "'%s' column is not in DataFrame, won't handle birth dates. Perhaps it was dropped because they contained null values only",
            keys.DATE_OF_BIRTH,
        )
        return False
    return True


def _will_handle_death_dates(df: pd.DataFrame) -> bool:
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

        # sanity check: itm should not be NaN
        if isinstance(itm, float) and pd.isna(itm):
            LOGGER.warning(
                "Unexpected 'NaN' value while converting lists of occupations to sets. Treating it as an empty set."
            )
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


def _tokenize_values(values, tokenize_func):
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
