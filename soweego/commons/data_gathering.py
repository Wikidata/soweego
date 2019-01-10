#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Gather relevant Wikidata and target catalog data for matching and validation purposes."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import re
from collections import defaultdict
from typing import Iterable, TypeVar

import regex
from sqlalchemy import or_

from soweego.commons import constants, url_utils
from soweego.commons.cache import cached
from soweego.commons.db_manager import DBManager
from soweego.wikidata import api_requests, sparql_queries, vocabulary

LOGGER = logging.getLogger(__name__)
T = TypeVar('T')


@cached
def gather_target_metadata(entity_type, catalog):
    catalog_constants = _get_catalog_constants(catalog)
    catalog_entity = _get_catalog_entity(entity_type, catalog_constants)

    LOGGER.info(
        'Gathering %s birth/death dates/places and gender metadata ...', catalog)
    entity = catalog_entity['entity']
    # Base metadata
    query_fields = _build_metadata_query_fields(entity, entity_type, catalog)

    session = DBManager.connect_to_db()
    query = session.query(
        *query_fields).filter(or_(entity.born.isnot(None), entity.died.isnot(None)))
    result = None
    try:
        raw_result = _run_query(query, catalog, entity_type)
        result = _parse_target_metadata_query_result(raw_result)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
    return result


def tokens_fulltext_search(target_entity: T, boolean_mode: bool, tokens: Iterable[str]) -> Iterable[T]:
    query = None
    if boolean_mode:
        query = ' '.join(map('+{0}'.format, tokens))
    else:
        query = ' '.join(tokens)

    ft_search = target_entity.tokens.match(query)

    session = DBManager.connect_to_db()
    result = []
    try:
        result = session.query(target_entity).filter(ft_search).all()
        session.commit()
    except:
        session.rollback()
        raise

    if not result:
        return []
    return result


def name_fulltext_search(target_entity: T, query: str) -> Iterable[T]:
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


def perfect_name_search(target_entity: T, to_search: str) -> Iterable[T]:
    session = DBManager.connect_to_db()
    try:
        for r in session.query(target_entity).filter(
                target_entity.name == to_search).all():
            yield r
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def gather_target_training_set(entity_type, catalog, identifiers):
    # TODO similar functions in constants
    catalog_constants = _get_catalog_constants(catalog)
    catalog_entity = _get_catalog_entity(entity_type, catalog_constants)

    LOGGER.info(
        'Gathering %s training set for the linker ...', catalog)
    base, link, nlp = catalog_entity['entity'], catalog_entity['link_entity'], catalog_entity['nlp_entity']

    session = DBManager.connect_to_db()
    query = session.query(base, link, nlp).join(link, base.catalog_id == link.catalog_id).join(
        nlp, base.catalog_id == nlp.catalog_id).filter(base.catalog_id.in_(identifiers))

    result = None
    try:
        raw_result = _run_query(query, catalog, entity_type)
        relevant_fields = _build_training_set_relevant_fields(base, link, nlp)
        result = _parse_target_training_set_query_result(
            raw_result, relevant_fields)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
    return result


def _build_training_set_relevant_fields(base, link, nlp):
    fields = set()
    for entity in base, link, nlp:
        for column in entity.__mapper__.column_attrs:
            field = column.key
            if field in ('internal_id', 'catalog_id'):
                continue
            fields.add(field)
    return fields


def _parse_target_training_set_query_result(result_set, relevant_fields):
    parsed = defaultdict(dict)
    for base, link, nlp in result_set:
        identifier = base.catalog_id
        for field in relevant_fields:
            if not parsed[identifier].get(field):
                parsed[identifier][field] = set()
            try:
                parsed[identifier][field].add(getattr(base, field))
            except AttributeError:
                pass
            try:
                parsed[identifier][field].add(getattr(link, field))
            except AttributeError:
                pass
            try:
                parsed[identifier][field].add(getattr(nlp, field))
            except AttributeError:
                pass
    return parsed


def _run_query(query, catalog, entity_type):
    count = query.count()
    if count == 0:
        LOGGER.warning(
            "No data available for %s %s. Stopping here", catalog, entity_type)
        return None
    LOGGER.info('Got %d entries with data from %s %s',
                count, catalog, entity_type)
    result_set = query.all()
    return result_set


def _build_metadata_query_fields(entity, entity_type, catalog):
    # Base metadata
    query_fields = [entity.catalog_id, entity.born,
                    entity.born_precision, entity.died, entity.died_precision]
    # Check additional metadata
    if hasattr(entity, 'gender'):
        query_fields.append(entity.gender)
    else:
        LOGGER.info('%s %s has no gender information', catalog, entity_type)
    if hasattr(entity, 'birth_place'):
        query_fields.append(entity.birth_place)
    else:
        LOGGER.info('%s %s has no birth place information',
                    catalog, entity_type)
    if hasattr(entity, 'death_place'):
        query_fields.append(entity.death_place)
    else:
        LOGGER.info('%s %s has no death place information',
                    catalog, entity_type)
    return query_fields


def _parse_target_metadata_query_result(result_set):
    for result in result_set:
        identifier = result.catalog_id
        born = result.born
        if born:
            # Default date precision when not available: 9 (year)
            born_precision = getattr(result, 'born_precision', 9)
            date_of_birth = f'{born}/{born_precision}'
            yield identifier, vocabulary.DATE_OF_BIRTH, date_of_birth
        else:
            LOGGER.debug('%s: no birth date available', identifier)
        died = result.died
        if died:
            died_precision = getattr(result, 'died_precision', 9)
            date_of_death = f'{died}/{died_precision}'
            yield identifier, vocabulary.DATE_OF_DEATH, date_of_death
        else:
            LOGGER.debug('%s: no death date available', identifier)
        if hasattr(result, 'gender'):
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


@cached
def gather_target_links(entity_type, catalog):
    catalog_constants = _get_catalog_constants(catalog)
    catalog_entity = _get_catalog_entity(entity_type, catalog_constants)

    LOGGER.info('Gathering %s %s links ...', catalog, entity_type)
    link_entity = catalog_entity['link_entity']

    session = DBManager.connect_to_db()
    result = None
    try:
        query = session.query(link_entity.catalog_id, link_entity.url)
        count = query.count()
        if count == 0:
            LOGGER.warning(
                "No links available for %s %s. Stopping validation here", catalog, entity_type)
            return None
        LOGGER.info('Got %d links from %s %s', count, catalog, entity_type)
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
        LOGGER.fatal('Bad entity type: %s. It should be one of %s',
                     entity_type, catalog_constants)
        raise ValueError('Bad entity type: %s. It should be one of %s' %
                         (entity_type, catalog_constants.keys()))
    return catalog_entity


def _get_catalog_constants(catalog):
    catalog_constants = constants.TARGET_CATALOGS.get(catalog)
    if not catalog_constants:
        LOGGER.fatal('Bad catalog: %s. It should be one of %s',
                     catalog, constants.TARGET_CATALOGS.keys())
        raise ValueError('Bad catalog: %s. It should be one of %s' %
                         (catalog, constants.TARGET_CATALOGS.keys()))
    return catalog_constants


def gather_wikidata_training_set(wikidata, url_pids, ext_id_pids_to_urls):
    LOGGER.info(
        'Gathering Wikidata training set from the Web API. This will take a while ...')
    total = 0

    for entity in api_requests.get_data_for_linker(wikidata.keys(), url_pids, ext_id_pids_to_urls):
        for qid, *data in entity:
            if len(data) == 1:  # URLs
                if not wikidata[qid].get(constants.DF_URL):
                    wikidata[qid][constants.DF_URL] = set()
                wikidata[qid][constants.DF_URL].add(data.pop())
            elif len(data) == 2:  # Statements
                pid, value = data
                pid_label = vocabulary.LINKER_PIDS.get(pid)
                if not pid_label:
                    LOGGER.fatal('PID label lookup failed: %s. The PID should be one of %s',
                                 pid, vocabulary.LINKER_PIDS.keys())
                    raise ValueError('PID label lookup failed: %s. The PID should be one of %s' % (
                        pid, vocabulary.LINKER_PIDS.keys()))
                parsed_value = _parse_wikidata_value(value)
                if not wikidata[qid].get(pid_label):
                    wikidata[qid][pid_label] = set()
                wikidata[qid][pid_label].add(parsed_value)
            elif len(data) == 3:  # Strings
                # TODO what to do with language codes?
                language, value, value_type = data
                if value_type == constants.DF_LABEL:
                    if not wikidata[qid].get(constants.DF_LABEL):
                        wikidata[qid][constants.DF_LABEL] = set()
                    wikidata[qid][constants.DF_LABEL].add(value)
                elif value_type == constants.DF_ALIAS:
                    if not wikidata[qid].get(constants.DF_ALIAS):
                        wikidata[qid][constants.DF_ALIAS] = set()
                    wikidata[qid][constants.DF_ALIAS].add(value)
                elif value_type == constants.DF_DESCRIPTION:
                    if not wikidata[qid].get(constants.DF_DESCRIPTION):
                        wikidata[qid][constants.DF_DESCRIPTION] = set()
                    wikidata[qid][constants.DF_DESCRIPTION].add(value)
                else:
                    expected_value_types = (
                        constants.DF_LABEL, constants.DF_ALIAS, constants.DF_DESCRIPTION)
                    LOGGER.fatal(
                        'Bad value type for Wikidata API result: %s. It should be one of %s', value_type, expected_value_types)
                    raise ValueError('Bad value type for Wikidata API result: %s. It should be one of %s' % (
                        value_type, expected_value_types))
            else:
                LOGGER.fatal(
                    'Bad size for Wikidata API result tuple: %d. It should be between 2 and 4. Tuple: %s', len(data) + 1, (qid, data))
                raise ValueError(
                    'Bad size for Wikidata API result tuple: %d. It should be between 2 and 4. Tuple: %s' % (len(data) + 1, (qid, data)))
            total += 1


def gather_wikidata_metadata(wikidata):
    LOGGER.info(
        'Gathering Wikidata birth/death dates/places and gender metadata from the Web API. This will take a while ...')
    total = 0
    # Generator of generators
    for entity in api_requests.get_metadata(wikidata.keys()):
        for qid, pid, value in entity:
            parsed = _parse_wikidata_value(value)
            if not wikidata[qid].get('metadata'):
                wikidata[qid]['metadata'] = set()
            wikidata[qid]['metadata'].add((pid, parsed))
            total += 1
    LOGGER.info('Got %d statements', total)


def _parse_wikidata_value(value):
    # Values: plain strings, monolingual strings, birth/death DATES, gender, birth/death places QIDs
    if isinstance(value, str):
        return value  # String
    date_value = value.get('time')
    if date_value:
        # +1180-01-01T00:00:00Z -> 1180-01-01
        parsed = date_value[1:].split('T')[0]
        return f"{parsed}/{value['precision']}"  # Date
    item_value = value.get('id')
    if item_value:
        return item_value  # QID
    monolingual_string_value = value.get('text')
    if monolingual_string_value:
        return monolingual_string_value  # Language string
    return None


def gather_wikidata_links(wikidata, url_pids, ext_id_pids_to_urls):
    LOGGER.info(
        'Gathering Wikidata sitelinks, third-party links, and external identifier links from the Web API. This will take a while ...')
    total = 0
    for iterator in api_requests.get_links(wikidata.keys(), url_pids, ext_id_pids_to_urls):
        for qid, url in iterator:
            if not wikidata[qid].get('links'):
                wikidata[qid]['links'] = set()
            wikidata[qid]['links'].add(url)
            total += 1
    LOGGER.info('Got %d links', total)


def gather_relevant_pids():
    url_pids = set()
    for result in sparql_queries.url_pids_query():
        url_pids.add(result)
    ext_id_pids_to_urls = defaultdict(dict)
    for result in sparql_queries.external_id_pids_and_urls_query():
        for pid, formatters in result.items():
            for formatter_url, formatter_regex in formatters.items():
                if formatter_regex:
                    try:
                        compiled_regex = re.compile(formatter_regex)
                    except re.error:
                        LOGGER.debug(
                            "Using 'regex' third-party library. Formatter regex not supported by the 're' standard library: %s", formatter_regex)
                        try:
                            compiled_regex = regex.compile(formatter_regex)
                        except regex.error:
                            LOGGER.debug(
                                "Giving up. Formatter regex not supported by 'regex': %s", formatter_regex)
                            compiled_regex = None
                else:
                    compiled_regex = None
                ext_id_pids_to_urls[pid][formatter_url] = compiled_regex
    return url_pids, ext_id_pids_to_urls


def gather_identifiers(entity, catalog, catalog_pid, aggregated):
    catalog_constants = _get_catalog_constants(catalog)
    LOGGER.info('Gathering Wikidata items with %s identifiers ...', catalog)
    query_type = 'identifier', constants.HANDLED_ENTITIES.get(entity)
    for result in sparql_queries.run_identifier_or_links_query(query_type, catalog_constants[entity]['qid'], catalog_pid, 0):
        for qid, target_id in result.items():
            if not aggregated.get(qid):
                aggregated[qid] = {'identifiers': set()}
            aggregated[qid]['identifiers'].add(target_id)
    LOGGER.info('Got %d %s identifiers', len(aggregated), catalog)


def extract_ids_from_urls(to_add, ext_id_pids_to_urls):
    LOGGER.info('Starting extraction of IDs from target links to be added ...')
    ext_ids_to_add = []
    urls_to_add = []
    for qid, urls in to_add.items():
        for url in urls:
            ext_id, pid = url_utils.get_external_id_from_url(
                url, ext_id_pids_to_urls)
            if ext_id:
                ext_ids_to_add.append((qid, pid, ext_id))
            else:
                urls_to_add.append(
                    (qid, vocabulary.DESCRIBED_AT_URL, url))
    return ext_ids_to_add, urls_to_add
