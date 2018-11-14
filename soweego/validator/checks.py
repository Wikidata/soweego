#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Validation of Wikidata statements against a target catalog"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
import re
from collections import defaultdict

import click
import regex
from sqlalchemy import or_

from soweego.commons import url_utils
from soweego.commons.cache import cached
from soweego.commons.constants import HANDLED_ENTITIES, TARGET_CATALOGS
from soweego.commons.db_manager import DBManager
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.musicbrainz_entity import (MusicbrainzArtistEntity,
                                                        MusicbrainzBandEntity)
from soweego.ingestor import wikidata_bot
from soweego.wikidata import api_requests, sparql_queries, vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('wikidata_query', type=click.Choice(['class', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('database_table')
@click.option('-o', '--outfile', type=click.File('w'), default='output/non_existent_ids.json', help="default: 'output/non_existent_ids.json'")
def check_existence_cli(wikidata_query, class_qid, catalog_pid, database_table, outfile):
    """Check the existence of identifier statements.

    Dump a JSON file of invalid ones ``{identifier: QID}``
    """
    entity = BaseEntity
    if database_table == 'musicbrainz_person':
        entity = MusicbrainzArtistEntity
    elif database_table == 'musicbrainz_band':
        entity = MusicbrainzBandEntity
    else:
        LOGGER.error('Not able to retrive entity for given database_table')

    invalid = check_existence(wikidata_query, class_qid,
                              catalog_pid, entity)
    json.dump(invalid, outfile, indent=2)


def check_existence(class_or_occupation_query, class_qid, catalog_pid, entity: BaseEntity):
    query_type = 'identifier', class_or_occupation_query
    session = _connect_to_db()
    invalid = defaultdict(set)
    count = 0

    for result in sparql_queries.run_identifier_or_links_query(query_type, class_qid, catalog_pid, 0):
        for qid, target_id in result.items():
            results = session.query(entity).filter(
                entity.catalog_id == target_id).all()
            if not results:
                LOGGER.warning(
                    '%s identifier %s is invalid', qid, target_id)
                invalid[target_id].add(qid)
                count += 1

    LOGGER.info('Total invalid identifiers = %d', count)
    # Sets are not serializable to JSON, so cast them to lists
    return {target_id: list(qids) for target_id, qids in invalid.items()}


def _connect_to_db():
    db_manager = DBManager()
    session = db_manager.new_session()
    return session


@click.command()
@click.argument('entity', type=click.Choice(HANDLED_ENTITIES.keys()))
@click.argument('catalog', type=click.Choice(TARGET_CATALOGS.keys()))
@click.option('--wikidata-dump/--no-wikidata-dump', default=False, help='Dump links gathered from Wikidata. Default: no.')
@click.option('--upload/--no-upload', default=True, help='Upload check results to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-c', '--cache', type=click.File(), default=None, help="Load Wikidata links previously dumped via '-w'. Default: no.")
@click.option('-d', '--deprecated', type=click.File('w'), default='output/links_deprecated_ids.json', help="Default: 'output/links_deprecated_ids.json'")
@click.option('-e', '--ext-ids', type=click.File('w'), default='output/external_ids_to_be_added.tsv', help="Default: 'output/external_ids_to_be_added.tsv'")
@click.option('-u', '--urls', type=click.File('w'), default='output/urls_to_be_added.tsv', help="Default: 'output/urls_to_be_added.tsv'")
@click.option('-w', '--wikidata', type=click.File('w'), default='output/wikidata_links.json', help="Default: 'output/wikidata_links.json'")
def check_links_cli(entity, catalog, wikidata_dump, upload, sandbox, cache, deprecated, ext_ids, urls, wikidata):
    """Check the validity of identifier statements based on the available links.

    Dump 3 output files:

    1. catalog identifiers to be deprecated, as a JSON ``{identifier: [QIDs]}``;

    2. external identifiers to be added, as a TSV ``QID  identifier_PID  identifier``;

    3. URLs to be added, as a TSV ``QID  P973   URL``.
    """
    if cache is None:
        to_deprecate, ext_ids_to_add, urls_to_add, wikidata_links = check_links(
            entity, catalog)
    else:
        wikidata_cache = _load_wikidata_cache(cache)
        to_deprecate, ext_ids_to_add, urls_to_add, wikidata_links = check_links(
            entity, catalog, wikidata_cache)

    if wikidata_dump:
        json.dump({qid: {data_type: list(values) for data_type, values in data.items()}
                   for qid, data in wikidata_links.items()}, wikidata, indent=2, ensure_ascii=False)
        LOGGER.info('Wikidata links dumped to %s', wikidata.name)
    if upload:
        _upload_links(catalog, to_deprecate, urls_to_add,
                      ext_ids_to_add, sandbox)

    json.dump({target_id: list(qids) for target_id,
               qids in to_deprecate.items()}, deprecated, indent=2)
    ext_ids.writelines(
        ['\t'.join(triple) + '\n' for triple in ext_ids_to_add])
    urls.writelines(
        ['\t'.join(triple) + '\n' for triple in urls_to_add])
    LOGGER.info('Result dumped to %s, %s, %s', deprecated.name,
                ext_ids.name, urls.name)


def check_links(entity, catalog, wikidata_cache=None):
    catalog_terms = _get_vocabulary(catalog)

    # Target links
    target = _gather_target_links(entity, catalog)
    # Early stop in case of no target links
    if target is None:
        return None, None, None

    to_deprecate = defaultdict(set)
    to_add = defaultdict(set)

    if wikidata_cache is None:
        wikidata = {}

        # Wikidata links
        _gather_identifiers(entity, catalog, catalog_terms['pid'], wikidata)
        url_pids, ext_id_pids_to_urls = _gather_relevant_pids()
        _gather_wikidata_links(wikidata, url_pids, ext_id_pids_to_urls)
    else:
        wikidata = wikidata_cache

    # Check
    _assess('links', wikidata, target, to_deprecate, to_add)

    # Separate external IDs from URLs
    ext_ids_to_add, urls_to_add = _extract_ids_from_urls(
        to_add, ext_id_pids_to_urls)

    LOGGER.info('Validation completed. %d %s IDs to be deprecated, %d external IDs to be added, %d URL statements to be added', len(
        to_deprecate), catalog, len(ext_ids_to_add), len(urls_to_add))

    return to_deprecate, ext_ids_to_add, urls_to_add, wikidata


@click.command()
@click.argument('entity', type=click.Choice(HANDLED_ENTITIES.keys()))
@click.argument('catalog', type=click.Choice(TARGET_CATALOGS.keys()))
@click.option('--wikidata-dump/--no-wikidata-dump', default=False, help='Dump metadata gathered from Wikidata. Default: no.')
@click.option('--upload/--no-upload', default=True, help='Upload check results to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-c', '--cache', type=click.File(), default=None, help="Load Wikidata metadata previously dumped via '-w'. Default: no.")
@click.option('-d', '--deprecated', type=click.File('w'), default='output/metadata_deprecated_ids.json', help="Default: 'output/metadata_deprecated_ids.json'")
@click.option('-a', '--added', type=click.File('w'), default='output/statements_to_be_added.tsv', help="Default: 'output/statements_to_be_added.tsv'")
@click.option('-w', '--wikidata', type=click.File('w'), default='output/wikidata_metadata.json', help="Default: 'output/wikidata_metadata.json'")
def check_metadata_cli(entity, catalog, wikidata_dump, upload, sandbox, cache, deprecated, added, wikidata):
    """Check the validity of identifier statements based on the availability
    of the following metadata: birth/death date, birth/death place, gender.

    Dump 2 output files:

    1. catalog identifiers to be deprecated, as a JSON ``{identifier: [QIDs]}``;

    2. statements to be added, as a TSV ``QID  metadata_PID  value``;
    """
    if cache:
        wikidata_cache = _load_wikidata_cache(cache)
        to_deprecate, to_add, wikidata_metadata = check_metadata(
            entity, catalog, wikidata_cache)
    else:
        to_deprecate, to_add, wikidata_metadata = check_metadata(
            entity, catalog)

    if wikidata_dump:
        json.dump({qid: {data_type: list(values) for data_type, values in data.items()}
                   for qid, data in wikidata_metadata.items()}, wikidata, indent=2, ensure_ascii=False)
        LOGGER.info('Wikidata metadata dumped to %s', wikidata.name)
    if upload:
        _upload(catalog, to_deprecate, to_add, sandbox)

    if to_deprecate:
        json.dump({target_id: list(qids) for target_id,
                   qids in to_deprecate.items()}, deprecated, indent=2)
    if to_add:
        added.writelines(
            ['\t'.join(triple) + '\n' for triple in to_add])

    LOGGER.info('Result dumped to %s, %s',
                deprecated.name, added.name)


def check_metadata(entity, catalog, wikidata_cache=None):
    catalog_terms = _get_vocabulary(catalog)

    # Target metadata
    target = _gather_target_metadata(entity, catalog)
    # Early stop in case of no target metadata
    if target is None:
        return None, None, None

    to_deprecate = defaultdict(set)
    to_add = defaultdict(set)

    if wikidata_cache is None:
        wikidata = {}

        # Wikidata metadata
        _gather_identifiers(entity, catalog, catalog_terms['pid'], wikidata)
        _gather_wikidata_metadata(wikidata)
    else:
        wikidata = wikidata_cache

    # Check
    _assess('metadata', wikidata, target, to_deprecate, to_add)

    return to_deprecate, to_add, wikidata


def _load_wikidata_cache(file_handle):
    raw_cache = json.load(file_handle)
    LOGGER.info("Loaded Wikidata cache from '%s'", file_handle.name)
    cache = {}
    for qid, data in raw_cache.items():
        for data_type, value_list in data.items():
            # Metadata has values that are a list
            if isinstance(value_list[0], list):
                value_set = set()
                for value in value_list:
                    value_set.add(tuple(value))
                if cache.get(qid):
                    cache[qid][data_type] = value_set
                else:
                    cache[qid] = {data_type: value_set}
            else:
                if cache.get(qid):
                    cache[qid][data_type] = set(value_list)
                else:
                    cache[qid] = {data_type: set(value_list)}
    return cache


@cached
def _gather_target_metadata(entity_type, catalog):
    catalog_constants = _get_catalog_constants(catalog)
    catalog_entity = _get_catalog_entity(entity_type, catalog_constants)

    LOGGER.info(
        'Gathering %s birth/death dates/places and gender metadata ...', catalog)
    entity = catalog_entity['entity']
    # Base metadata
    query_fields = _build_metadata_query_fields(entity, entity_type, catalog)

    session = _connect_to_db()
    result = None
    try:
        result = _run_metadata_query(
            session, query_fields, entity, catalog, entity_type)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    if not result:
        return None
    return _parse_target_metadata_query_result(result)


def _run_metadata_query(session, query_fields, entity, catalog, entity_type):
    query = session.query(
        *query_fields).filter(or_(entity.born.isnot(None), entity.died.isnot(None)))
    count = query.count()
    if count == 0:
        LOGGER.warning(
            "No metadata available for %s %s. Stopping validation here", catalog, entity_type)
        return None
    LOGGER.info('Got %d entries with metadata from %s %s',
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


# Default date precision when not available: 9 (year)
def _parse_target_metadata_query_result(result_set):
    for result in result_set:
        identifier = result.catalog_id
        born = result.born
        if born:
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
def _gather_target_links(entity_type, catalog):
    catalog_constants = _get_catalog_constants(catalog)
    catalog_entity = _get_catalog_entity(entity_type, catalog_constants)

    LOGGER.info('Gathering %s %s links ...', catalog, entity_type)
    link_entity = catalog_entity['link_entity']

    session = _connect_to_db()
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


def _get_catalog_entity(entity, catalog_constants):
    catalog_entity = catalog_constants.get(entity)
    if not catalog_entity:
        raise ValueError('Bad entity type: %s. Please use one of %s' %
                         (entity, catalog_constants.keys()))
    return catalog_entity


def _assess(criterion, source, target_iterator, to_deprecate, to_add):
    LOGGER.info('Starting check against target %s ...', criterion)
    target = _consume_target_iterator(target_iterator)
    # Large loop size = # given Wikidata class instances with identifiers, e.g., 80k musicians
    for qid, data in source.items():
        source_data = data.get(criterion)
        if not source_data:
            LOGGER.warning(
                'Skipping check: no %s available in QID %s', criterion, qid)
            continue
        identifiers = data['identifiers']
        # 1 or tiny loop size = # of identifiers per Wikidata item (should always be 1)
        for target_id in identifiers:
            if target_id in target.keys():
                target_data = target.get(target_id)
                if not target_data:
                    LOGGER.warning(
                        'Skipping check: no %s available in target ID %s', criterion, target_id)
                    continue
                shared = source_data.intersection(target_data)
                extra = target_data.difference(source_data)
                if not shared:
                    LOGGER.debug(
                        'No shared %s between %s and %s. The identifier statement will be deprecated', criterion, qid, target_id)
                    to_deprecate[target_id].add(qid)
                else:
                    LOGGER.debug('%s and %s share these %s: %s',
                                 qid, target_id, criterion, shared)
                if extra:
                    LOGGER.debug(
                        '%s has extra %s that will be added to %s: %s', target_id, criterion, qid, extra)
                    to_add[qid].update(extra)
                else:
                    LOGGER.debug('%s has no extra %s', target_id, criterion)
    LOGGER.info('Check against target %s completed: %d IDs to be deprecated, %d statements to be added',
                criterion, len(to_deprecate), len(to_add))


def _consume_target_iterator(target_iterator):
    target = defaultdict(set)
    for identifier, *data in target_iterator:
        if len(data) == 1:  # Links
            target[identifier].add(data.pop())
        else:  # Metadata
            target[identifier].add(tuple(data))
    return target


def _extract_ids_from_urls(to_add, ext_id_pids_to_urls):
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


def _get_vocabulary(catalog):
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog)
    if not catalog_terms:
        raise ValueError('Bad catalog: %s. Please use one of %s' %
                         (catalog, vocabulary.CATALOG_MAPPING.keys()))
    return catalog_terms


def _get_catalog_constants(catalog):
    catalog_constants = TARGET_CATALOGS.get(catalog)
    if not catalog_constants:
        raise ValueError('Bad catalog: %s. Please use on of %s' %
                         (catalog, TARGET_CATALOGS.keys()))
    return catalog_constants


def _gather_wikidata_metadata(wikidata):
    LOGGER.info(
        'Gathering Wikidata birth/death dates/places and gender metadata. This will take a while ...')
    total = 0
    # Generator of generators
    for entity in api_requests.get_metadata(wikidata.keys()):
        for qid, pid, value in entity:
            parsed = _parse_wikidata_metadata_value(value)
            if not wikidata[qid].get('metadata'):
                wikidata[qid]['metadata'] = set()
            wikidata[qid]['metadata'].add((pid, parsed))
            total += 1
    LOGGER.info('Got %d statements', total)


def _parse_wikidata_metadata_value(value):
    # Values: birth/death DATES, gender, birth/death places QIDs
    date_value = value.get('time')
    if date_value:
        # +1180-01-01T00:00:00Z -> 1180-01-01
        parsed = date_value[1:].split('T')[0]
        return f"{parsed}/{value['precision']}"  # Date
    item_value = value.get('id')
    if item_value:
        return item_value  # QID
    return value  # String


def _gather_wikidata_links(wikidata, url_pids, ext_id_pids_to_urls):
    LOGGER.info(
        'Gathering Wikidata sitelinks, third-party links, and external identifier links. This will take a while ...')
    total = 0
    for iterator in api_requests.get_links(wikidata.keys(), url_pids, ext_id_pids_to_urls):
        for qid, url in iterator:
            if not wikidata[qid].get('links'):
                wikidata[qid]['links'] = set()
            wikidata[qid]['links'].add(url)
            total += 1
    LOGGER.info('Got %d links', total)


def _gather_relevant_pids():
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
                        compiled_regex = regex.compile(formatter_regex)
                else:
                    compiled_regex = None
                ext_id_pids_to_urls[pid][formatter_url] = compiled_regex
    return url_pids, ext_id_pids_to_urls


def _gather_identifiers(entity, catalog, catalog_pid, aggregated):
    catalog_constants = _get_catalog_constants(catalog)
    LOGGER.info('Gathering Wikidata items with %s identifiers ...', catalog)
    query_type = 'identifier', HANDLED_ENTITIES.get(entity)
    for result in sparql_queries.run_identifier_or_links_query(query_type, catalog_constants[entity]['qid'], catalog_pid, 0):
        for qid, target_id in result.items():
            if not aggregated.get(qid):
                aggregated[qid] = {'identifiers': set()}
            aggregated[qid]['identifiers'].add(target_id)
    LOGGER.info('Got %d %s identifiers', len(aggregated), catalog)


def _upload_links(catalog, to_deprecate, urls_to_add, ext_ids_to_add, sandbox):
    catalog_qid = _upload(catalog, to_deprecate, urls_to_add, sandbox)
    LOGGER.info('Starting addition of external IDs to Wikidata ...')
    wikidata_bot.add_statements(ext_ids_to_add, catalog_qid, sandbox)


def _upload(catalog, to_deprecate, to_add, sandbox):
    catalog_terms = _get_vocabulary(catalog)
    catalog_qid = catalog_terms['qid']
    LOGGER.info('Starting deprecation of %s IDs ...', catalog)
    wikidata_bot.delete_or_deprecate_identifiers(
        'deprecate', to_deprecate, catalog, sandbox)
    LOGGER.info('Starting addition of statements to Wikidata ...')
    wikidata_bot.add_statements(to_add, catalog_qid, sandbox)
    return catalog_qid
