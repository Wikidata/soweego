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
from collections import defaultdict

import click

from soweego.commons import data_gathering, target_database, constants
from soweego.commons.db_manager import DBManager
from soweego.ingestor import wikidata_bot

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('entity', type=click.Choice(constants.HANDLED_ENTITIES.keys()))
@click.argument('catalog', type=click.Choice(constants.TARGET_CATALOGS.keys()))
@click.option('--wikidata-dump/--no-wikidata-dump', default=False, help='Dump links gathered from Wikidata. Default: no.')
@click.option('--upload/--no-upload', default=True, help='Upload check results to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-c', '--cache', type=click.File(), default=None, help="Load Wikidata links previously dumped via '-w'. Default: no.")
@click.option('-d', '--deprecated', type=click.File('w'), default=constants.SHARED_FOLDER + 'entities_deprecated_ids.json',
              help="Default: '%sentities_deprecated_ids.json'" % constants.SHARED_FOLDER)
@click.option('-w', '--wikidata', type=click.File('w'), default=constants.SHARED_FOLDER + 'wikidata_entities.json',
              help="Default: '%swikidata_entities.json'" % constants.SHARED_FOLDER)
def check_existence_cli(entity, catalog, wikidata_dump, upload, sandbox, cache, deprecated, wikidata):
    """Check the existence of identifier statements.

    Dump a JSON file of invalid ones ``{identifier: QID}``
    """
    if cache is None:
        invalid, wikidata_cache = check_existence(entity, catalog)
    else:
        wikidata_cache = _load_wikidata_cache(cache)
        invalid, wikidata = check_existence(
            entity, catalog, wikidata_cache=wikidata_cache)

    if wikidata_dump:
        json.dump({qid: {data_type: list(values) for data_type, values in data.items()}
                   for qid, data in wikidata_cache.items()}, wikidata, indent=2, ensure_ascii=False)
        LOGGER.info('Wikidata metadata dumped to %s', wikidata.name)

    if upload:
        _upload_links(catalog, invalid, None, None, sandbox)

    invalid = {target_id: list(qids) for target_id, qids in invalid.items()}
    json.dump(invalid, deprecated, indent=2)
    LOGGER.info('Result dumped to %s', deprecated.name)


def check_existence(entity, catalog, wikidata_cache=None):

    if wikidata_cache is None:
        wikidata = {}

        pid = target_database.get_pid(catalog)
        data_gathering.gather_target_ids(entity, catalog, pid, wikidata)
    else:
        wikidata = wikidata_cache

    session = DBManager.connect_to_db()
    invalid = defaultdict(set)
    count = 0
    entity = target_database.get_entity(catalog, entity)

    for qid in wikidata:
        identifiers = wikidata[qid][constants.TID]
        for target_id in identifiers:
            results = session.query(entity).filter(
                entity.catalog_id == target_id).all()
            if not results:
                LOGGER.info('%s %s identifier %s is invalid',
                            qid, catalog, target_id)
                invalid[target_id].add(qid)
                count += 1

    LOGGER.info('Total invalid identifiers = %d', count)
    # Sets are not serializable to JSON, so cast them to lists
    return invalid, wikidata


@click.command()
@click.argument('entity', type=click.Choice(constants.HANDLED_ENTITIES.keys()))
@click.argument('catalog', type=click.Choice(constants.TARGET_CATALOGS.keys()))
@click.option('--wikidata-dump/--no-wikidata-dump', default=False, help='Dump links gathered from Wikidata. Default: no.')
@click.option('--upload/--no-upload', default=True, help='Upload check results to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-c', '--cache', type=click.File(), default=None, help="Load Wikidata links previously dumped via '-w'. Default: no.")
@click.option('-d', '--deprecated', type=click.File('w'), default=constants.SHARED_FOLDER + 'links_deprecated_ids.json',
              help="Default: '%slinks_deprecated_ids.json'" % constants.SHARED_FOLDER)
@click.option('-e', '--ext-ids', type=click.File('w'), default=constants.SHARED_FOLDER + 'external_ids_to_be_added.tsv',
              help="Default: '%sexternal_ids_to_be_added.tsv'" % constants.SHARED_FOLDER)
@click.option('-u', '--urls', type=click.File('w'), default=constants.SHARED_FOLDER + 'urls_to_be_added.tsv',
              help="Default: '%surls_to_be_added.tsv'" % constants.SHARED_FOLDER)
@click.option('-w', '--wikidata', type=click.File('w'), default=constants.SHARED_FOLDER + 'wikidata_links.json',
              help="Default: '%swikidata_links.json'" % constants.SHARED_FOLDER)
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

    if to_deprecate is None:
        return

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
    pid = target_database.get_pid(catalog)

    # Target links
    target = data_gathering.gather_target_links(entity, catalog)
    # Early stop in case of no target links
    if target is None:
        return None, None, None, None

    to_deprecate = defaultdict(set)
    to_add = defaultdict(set)

    if wikidata_cache is None:
        wikidata = {}

        # Wikidata links
        data_gathering.gather_target_ids(entity, catalog, pid, wikidata)
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        data_gathering.gather_wikidata_links(
            wikidata, url_pids, ext_id_pids_to_urls)
    else:
        wikidata = wikidata_cache

    # Check
    _assess('links', wikidata, target, to_deprecate, to_add)

    # Separate external IDs from URLs
    ext_ids_to_add, urls_to_add = data_gathering.extract_ids_from_urls(
        to_add, ext_id_pids_to_urls)

    LOGGER.info('Validation completed. %d %s IDs to be deprecated, %d external IDs to be added, %d URL statements to be added', len(
        to_deprecate), catalog, len(ext_ids_to_add), len(urls_to_add))

    return to_deprecate, ext_ids_to_add, urls_to_add, wikidata


@click.command()
@click.argument('entity', type=click.Choice(constants.HANDLED_ENTITIES.keys()))
@click.argument('catalog', type=click.Choice(constants.TARGET_CATALOGS.keys()))
@click.option('--wikidata-dump/--no-wikidata-dump', default=False, help='Dump metadata gathered from Wikidata. Default: no.')
@click.option('--upload/--no-upload', default=True, help='Upload check results to Wikidata. Default: yes.')
@click.option('--sandbox/--no-sandbox', default=False, help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-c', '--cache', type=click.File(), default=None, help="Load Wikidata metadata previously dumped via '-w'. Default: no.")
@click.option('-d', '--deprecated', type=click.File('w'), default=constants.SHARED_FOLDER + 'metadata_deprecated_ids.json',
              help="Default: '%smetadata_deprecated_ids.json'" % constants.SHARED_FOLDER)
@click.option('-a', '--added', type=click.File('w'), default=constants.SHARED_FOLDER + 'statements_to_be_added.tsv',
              help="Default: '%sstatements_to_be_added.tsv'" % constants.SHARED_FOLDER)
@click.option('-w', '--wikidata', type=click.File('w'), default=constants.SHARED_FOLDER + 'wikidata_metadata.json',
              help="Default: '%swikidata_metadata.json'" % constants.SHARED_FOLDER)
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

    if to_deprecate is None:
        return

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
    # Target metadata
    target = data_gathering.gather_target_metadata(entity, catalog)
    # Early stop in case of no target metadata
    if target is None:
        return None, None, None

    to_deprecate = defaultdict(set)
    to_add = defaultdict(set)

    if wikidata_cache is None:
        wikidata = {}

        # Wikidata metadata
        data_gathering.gather_target_ids(
            entity, catalog, target_database.get_pid(catalog), wikidata)
        data_gathering.gather_wikidata_metadata(wikidata)
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


def _assess(criterion, source, target_generator, to_deprecate, to_add):
    LOGGER.info('Starting check against target %s ...', criterion)
    target = _consume_target_generator(target_generator)
    # Large loop size = # given Wikidata class instances with identifiers, e.g., 80k musicians
    for qid, data in source.items():
        source_data = data.get(criterion)
        if not source_data:
            LOGGER.warning(
                'Skipping check: no %s available in QID %s', criterion, qid)
            continue
        identifiers = data[constants.TID]
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


def _consume_target_generator(target_generator):
    target = defaultdict(set)
    for identifier, *data in target_generator:
        if len(data) == 1:  # Links
            target[identifier].add(data.pop())
        else:  # Metadata
            target[identifier].add(tuple(data))
    return target


def _upload_links(catalog, to_deprecate, urls_to_add, ext_ids_to_add, sandbox):
    catalog_qid = _upload(catalog, to_deprecate, urls_to_add, sandbox)
    LOGGER.info('Starting addition of external IDs to Wikidata ...')
    wikidata_bot.add_statements(ext_ids_to_add, catalog_qid, sandbox)


def _upload(catalog, to_deprecate, to_add, sandbox):
    catalog_qid = target_database.get_qid(catalog)
    LOGGER.info('Starting deprecation of %s IDs ...', catalog)
    wikidata_bot.delete_or_deprecate_identifiers(
        'deprecate', to_deprecate, catalog, sandbox)
    LOGGER.info('Starting addition of statements to Wikidata ...')
    wikidata_bot.add_statements(to_add, catalog_qid, sandbox)
    return catalog_qid
