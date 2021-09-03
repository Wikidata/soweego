#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Gather relevant Wikidata and target catalog data for matching and validation purposes."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import datetime
import json
import logging
import re
from collections import defaultdict
from typing import Iterable

import regex
from sqlalchemy import or_
from tqdm import tqdm

from soweego.commons import (
    constants,
    keys,
    target_database,
    text_utils,
    url_utils,
)
from soweego.commons.db_manager import DBManager
from soweego.importer import models
from soweego.wikidata import api_requests, sparql_queries, vocabulary

LOGGER = logging.getLogger(__name__)


def gather_target_biodata(entity, catalog):
    LOGGER.info(
        'Gathering %s birth/death dates/places and gender metadata ...', catalog
    )
    db_entity = target_database.get_main_entity(catalog, entity)
    # Base biodata
    query_fields = _build_biodata_query_fields(db_entity, entity, catalog)

    session = DBManager.connect_to_db()
    query = session.query(*query_fields).filter(
        or_(db_entity.born.isnot(None), db_entity.died.isnot(None))
    )
    result = None
    try:
        raw_result = _run_query(query, catalog, entity)
        if raw_result is None:
            return None
        result = _parse_target_biodata_query_result(raw_result)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
    return result


def tokens_fulltext_search(
    target_entity: constants.DB_ENTITY,
    boolean_mode: bool,
    tokens: Iterable[str],
    where_clause=None,
    limit: int = 10,
) -> Iterable[constants.DB_ENTITY]:
    if issubclass(target_entity, models.base_entity.BaseEntity):
        column = target_entity.name_tokens
    elif issubclass(target_entity, models.base_link_entity.BaseLinkEntity):
        column = target_entity.url_tokens
    elif issubclass(target_entity, models.base_nlp_entity.BaseNlpEntity):
        column = target_entity.description_tokens
    else:
        LOGGER.critical('Bad target entity class: %s', target_entity)
        raise ValueError('Bad target entity class: %s' % target_entity)

    tokens = filter(None, tokens)
    terms = (
        ' '.join(map('+{0}'.format, tokens))
        if boolean_mode
        else ' '.join(tokens)
    )
    ft_search = column.match(terms)

    session = DBManager.connect_to_db()
    try:
        if where_clause is None:
            query = session.query(target_entity).filter(ft_search).limit(limit)
        else:
            query = (
                session.query(target_entity)
                .filter(ft_search)
                .filter(where_clause)
                .limit(limit)
            )

        count = query.count()
        if count == 0:
            LOGGER.debug(
                "No result from full-text index query to %s. Terms: '%s'",
                target_entity.__name__,
                terms,
            )
            session.commit()
        else:
            for row in query:
                yield row
            session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def name_fulltext_search(
    target_entity: constants.DB_ENTITY, query: str
) -> Iterable[constants.DB_ENTITY]:
    ft_search = target_entity.name.match(query)

    session = DBManager.connect_to_db()
    try:
        for r in session.query(target_entity).filter(ft_search).all():
            yield r
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def perfect_name_search(
    target_entity: constants.DB_ENTITY, to_search: str
) -> Iterable[constants.DB_ENTITY]:
    session = DBManager.connect_to_db()
    try:
        for r in (
            session.query(target_entity)
            .filter(target_entity.name == to_search)
            .all()
        ):
            yield r

    except:
        session.rollback()
        raise
    finally:
        session.close()


def perfect_name_search_bucket(
    target_entity: constants.DB_ENTITY, to_search: set
) -> Iterable[constants.DB_ENTITY]:
    session = DBManager.connect_to_db()
    try:
        for r in (
            session.query(target_entity)
            .filter(target_entity.name.in_(to_search))
            .all()
        ):
            yield r

    except:
        session.rollback()
        raise
    finally:
        session.close()


def _build_dataset_relevant_fields(base, link, nlp):
    fields = set()
    for entity in base, link, nlp:

        # if either of (base, link, nlp) is None
        if not entity:
            continue

        for column in entity.__mapper__.column_attrs:
            field = column.key
            if field in ('internal_id', 'catalog_id'):
                continue
            fields.add(field)
    return fields


def _dump_target_dataset_query_result(
    result, relevant_fields, fileout, chunk_size=1000
):
    chunk = []

    for res in result:

        # if it is only `base` then we convert is to a list
        # so that we can reuse the same algorithm
        if not isinstance(res, tuple):
            res = [res]

        # the first item of the list is always `base`
        base = res[0]
        parsed = {keys.TID: base.catalog_id}

        for field in relevant_fields:

            # for every `table` in the results
            for tb in res:

                # we try to get the appropriate field for that table
                try:
                    f_value = getattr(tb, field)

                    # if the value is a date/datetime then we need
                    # to convert it to string, so that it is JSON
                    # serializable
                    if isinstance(f_value, (datetime.date, datetime.datetime)):
                        parsed[field] = f_value.isoformat()

                    else:
                        parsed[field] = f_value

                except AttributeError:
                    pass

        fileout.write(json.dumps(parsed, ensure_ascii=False) + '\n')
        fileout.flush()

        if len(chunk) <= chunk_size:
            chunk.append(parsed)
        else:
            yield chunk
            chunk = []


def _run_query(query, catalog, entity_type, page=1000):
    count = query.count()
    if count == 0:
        LOGGER.warning(
            "No data available for %s %s. Stopping here", catalog, entity_type
        )
        return None
    LOGGER.info(
        'Got %d internal IDs with data from %s %s', count, catalog, entity_type
    )
    return query.yield_per(page).enable_eagerloads(False)


def _build_biodata_query_fields(entity, entity_type, catalog):
    # Base biographical data
    query_fields = [
        entity.catalog_id,
        entity.born,
        entity.born_precision,
        entity.died,
        entity.died_precision,
    ]
    # Check additional data
    if hasattr(entity, 'gender'):
        query_fields.append(entity.gender)
    else:
        LOGGER.info('%s %s has no gender information', catalog, entity_type)
    if hasattr(entity, 'birth_place'):
        query_fields.append(entity.birth_place)
    else:
        LOGGER.info(
            '%s %s has no birth place information', catalog, entity_type
        )
    if hasattr(entity, 'death_place'):
        query_fields.append(entity.death_place)
    else:
        LOGGER.info(
            '%s %s has no death place information', catalog, entity_type
        )
    return query_fields


def _parse_target_biodata_query_result(result_set):
    for result in result_set:
        identifier = result.catalog_id
        born = result.born
        if born:
            # Default date precision when not available: 9 (year)
            born_precision = getattr(result, 'born_precision', 9)
            yield identifier, vocabulary.DATE_OF_BIRTH, f'{born}/{born_precision}'
        else:
            LOGGER.debug('%s: no birth date available', identifier)
        died = result.died
        if died:
            died_precision = getattr(result, 'died_precision', 9)
            yield identifier, vocabulary.DATE_OF_DEATH, f'{died}/{died_precision}'
        else:
            LOGGER.debug('%s: no death date available', identifier)
        if hasattr(result, 'gender') and result.gender is not None:
            yield identifier, vocabulary.SEX_OR_GENDER, result.gender
        else:
            LOGGER.debug('%s: no gender available', identifier)
        if hasattr(result, 'birth_place'):
            yield identifier, vocabulary.PLACE_OF_BIRTH, result.birth_place
        else:
            LOGGER.debug('%s: no birth place available', identifier)
        if hasattr(result, 'death_place'):
            yield identifier, vocabulary.PLACE_OF_DEATH, result.death_place
        else:
            LOGGER.debug('%s: no death place available', identifier)


def gather_target_links(entity, catalog):
    LOGGER.info('Gathering %s %s links ...', catalog, entity)
    link_entity = target_database.get_link_entity(catalog, entity)

    # Early return when the links table doesn't exist
    if link_entity is None:
        LOGGER.warning(
            'No links table available in the database for %s %s. '
            'Stopping validation here',
            catalog,
            entity,
        )
        return None

    session = DBManager.connect_to_db()
    result = None
    try:
        query = session.query(link_entity.catalog_id, link_entity.url)
        count = query.count()
        # Early return when no links
        if count == 0:
            LOGGER.warning(
                'No links available for %s %s. Stopping validation here',
                catalog,
                entity,
            )
            return None
        LOGGER.info('Got %d links from %s %s', count, catalog, entity)
        result = query.all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    if result is None:
        return None
    for row in result:
        yield row.catalog_id, row.url


def _get_catalog_entity(entity_type, catalog_constants):
    catalog_entity = catalog_constants.get(entity_type)
    if not catalog_entity:
        LOGGER.critical(
            'Bad entity type: %s. It should be one of %s',
            entity_type,
            catalog_constants,
        )
        raise ValueError(
            'Bad entity type: %s. It should be one of %s'
            % (entity_type, catalog_constants.keys())
        )
    return catalog_entity


def _get_catalog_constants(catalog):
    catalog_constants = constants.TARGET_CATALOGS.get(catalog)
    if not catalog_constants:
        LOGGER.critical(
            'Bad catalog: %s. It should be one of %s',
            catalog,
            constants.TARGET_CATALOGS.keys(),
        )
        raise ValueError(
            'Bad catalog: %s. It should be one of %s'
            % (catalog, constants.TARGET_CATALOGS.keys())
        )
    return catalog_constants


def gather_wikidata_biodata(wikidata):
    LOGGER.info(
        'Gathering Wikidata birth/death dates/places and gender data from the Web API. This will take a while ...'
    )
    total = 0

    for qid, pid, value in api_requests.get_biodata(wikidata.keys()):
        parsed = api_requests.parse_value(value)
        if not wikidata[qid].get(keys.BIODATA):
            wikidata[qid][keys.BIODATA] = []
        # If `parsed` is a set, we have item labels,
        # see `api_requests.parse_value` behavior
        if isinstance(parsed, set):
            # Keep track of the value QID
            # Dict key checks are already done in `api_requests.parse_value`,
            # so no need to redo it here
            v_qid = value['id']
            # Normalize & de-duplicate labels
            # `text_utils.normalize` returns a tuple with two forms
            # (non-lower, lower): take the lowercased one
            labels = {text_utils.normalize(label)[1] for label in parsed}
            # e.g., (P19, Q641, {'venezia', 'venice', ...})
            wikidata[qid][keys.BIODATA].append((pid, v_qid, labels))
        # If `parsed` is a tuple, we have a (timestamp, precision) date
        elif isinstance(parsed, tuple):
            timestamp, precision = parsed[0], parsed[1]
            # Get rid of time, useless
            timestamp = timestamp.split('T')[0]
            wikidata[qid][keys.BIODATA].append(
                (pid, f'{timestamp}/{precision}')
            )
        else:
            wikidata[qid][keys.BIODATA].append((pid, parsed))
        total += 1

    LOGGER.info('Got %d statements', total)


def gather_wikidata_links(wikidata, url_pids, ext_id_pids_to_urls):
    LOGGER.info(
        'Gathering Wikidata sitelinks, third-party links, and external identifier links from the Web API. This will take a while ...'
    )
    total = 0
    for generator in api_requests.get_links(
        wikidata.keys(), url_pids, ext_id_pids_to_urls
    ):
        for qid, url in generator:
            if not wikidata[qid].get(keys.LINKS):
                wikidata[qid][keys.LINKS] = set()
            wikidata[qid][keys.LINKS].add(url)
            total += 1
    LOGGER.info('Got %d links', total)


def gather_relevant_pids():
    url_pids = set()
    for result in sparql_queries.url_pids():
        url_pids.add(result)

    ext_id_pids_to_urls = defaultdict(dict)
    for (
        pid,
        formatter_url,
        id_regex,
        url_regex,
    ) in sparql_queries.external_id_pids_and_urls():
        compiled_id_regex = _compile(id_regex, 'ID')
        compiled_url_regex = _compile(url_regex, 'URL')

        ext_id_pids_to_urls[pid][formatter_url] = (
            compiled_id_regex,
            compiled_url_regex,
        )

    return url_pids, ext_id_pids_to_urls


def _compile(regexp, id_or_url):
    if regexp is None:
        return None

    try:
        compiled = re.compile(regexp)
    except re.error:
        LOGGER.debug(
            "Using 'regex' third-party library. %s regex not supported by the 're' standard library: %s",
            id_or_url,
            regexp,
        )
        try:
            compiled = regex.compile(regexp)
        except regex.error:
            LOGGER.debug(
                "Giving up. %s regex not supported by 'regex': %s",
                id_or_url,
                regexp,
            )
            return None

    return compiled


def gather_target_ids(entity, catalog, catalog_pid, aggregated):
    LOGGER.info(
        'Gathering Wikidata %s items with %s identifiers ...', entity, catalog
    )

    query_type = keys.IDENTIFIER, constants.SUPPORTED_ENTITIES.get(entity)

    for qid, target_id in sparql_queries.run_query(
        query_type,
        target_database.get_class_qid(catalog, entity),
        catalog_pid,
        0,
    ):
        if not aggregated.get(qid):
            aggregated[qid] = {keys.TID: set()}
        aggregated[qid][keys.TID].add(target_id)

    LOGGER.info('Got %d %s identifiers', len(aggregated), catalog)


def gather_qids(entity, catalog, catalog_pid):
    LOGGER.info(
        'Gathering Wikidata %s items with no %s identifiers ...',
        entity,
        catalog,
    )
    query_type = keys.DATASET, constants.SUPPORTED_ENTITIES.get(entity)
    qids = set(
        sparql_queries.run_query(
            query_type,
            target_database.get_class_qid(catalog, entity),
            catalog_pid,
            0,
        )
    )
    LOGGER.info('Got %d Wikidata items', len(qids))
    return qids


def extract_ids_from_urls(to_be_added, ext_id_pids_to_urls):
    LOGGER.info('Starting extraction of IDs from target links to be added ...')
    ext_ids_to_add = []
    urls_to_add = []
    for (qid, tid,), urls in tqdm(to_be_added.items(), total=len(to_be_added)):
        for url in urls:
            (ext_id, pid,) = url_utils.get_external_id_from_url(
                url, ext_id_pids_to_urls
            )
            if ext_id is not None:
                ext_ids_to_add.append((qid, pid, ext_id, tid,))
            else:
                urls_to_add.append((qid, vocabulary.EXACT_MATCH, url, tid,))
    return (
        ext_ids_to_add,
        urls_to_add,
    )
