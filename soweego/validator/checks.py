#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
from collections import defaultdict

import click

from soweego.wikidata import api_requests, sparql_queries, vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('class_or_occupation_query', type=click.Choice(['class', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('target_identifiers', type=click.File())
@click.option('-o', '--outfile', type=click.File('w'), default='output/non_existent_ids.json', help="default: 'output/non_existent_ids.json'")
def check_existence_cli(class_or_occupation_query, class_qid, catalog_pid, target_identifiers, outfile):
    """Check the existence of identifier statements.

    Dump a JSON file of invalid ones ``{identifier: QID}``
    """
    invalid = check_existence(class_or_occupation_query, class_qid,
                              catalog_pid, target_identifiers)
    json.dump(invalid, outfile, indent=2)


def check_existence(class_or_occupation_query, class_qid, catalog_pid, target_identifiers):
    # TODO for each wikidata_items item, do a binary search on the target list
    query_type = 'identifier', class_or_occupation_query
    target_ids_set = set(target_id.rstrip()
                         for target_id in target_identifiers)
    invalid = defaultdict(set)
    count = 0
    for qid, target_id in sparql_queries.run_identifier_or_links_query(query_type, class_qid, catalog_pid, 0):
        if target_id not in target_ids_set:
            LOGGER.warning(
                '%s identifier %s is invalid', qid, target_id)
            invalid[target_id].add(qid)
            count += 1
    LOGGER.info('Total invalid identifiers = %d', count)
    # Sets are not serializable to JSON, so cast them to lists
    return {target_id: list(qids) for target_id, qids in invalid.items()}


@click.command()
@click.argument('class_or_occupation_query', type=click.Choice(['class', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('target_links', type=click.File())
@click.option('-d', '--deprecated-outfile', type=click.File('w'), default='output/deprecated_ids.json', help="default: 'output/deprecated_ids.json'")
@click.option('-l', '--links-outfile', type=click.File('w'), default='output/links_to_be_added.json', help="default: 'output/links_to_be_added.json'")
def check_links_cli(class_or_occupation_query, class_qid, catalog_pid, target_links, deprecated_outfile, links_outfile):
    """Check the validity of identifier statements based on the available links.

    Dump 2 JSON files: deprecated identifiers ``{identifer: QID}``
    and links to be added ``{QID: [links]}``
    """
    loaded_target_links = json.load(target_links)
    LOGGER.info("Loaded target links file '%s'", target_links.name)
    deprecated, links_to_be_added = check_links(
        class_or_occupation_query, class_qid, catalog_pid, loaded_target_links)
    json.dump(deprecated, deprecated_outfile, indent=2)
    json.dump(links_to_be_added, links_outfile, indent=2)


def check_links(class_or_occupation_query, class_qid, catalog_pid, target_ids_to_links):
    aggregated = {}
    deprecated = defaultdict(set)
    to_be_added = defaultdict(set)
    query_type = 'identifier', class_or_occupation_query
    LOGGER.info('Gathering %s external identifiers ...', catalog_pid)
    for result in sparql_queries.run_identifier_or_links_query(query_type, class_qid, catalog_pid, 0):
        for qid, target_id in result.items():
            if not aggregated.get(qid):
                aggregated[qid] = {'identifiers': set()}
            aggregated[qid]['identifiers'].add(target_id)
    query_type = 'links', class_or_occupation_query
    LOGGER.info('Got %d %s external identifiers', len(aggregated), catalog_pid)
    LOGGER.info(
        'Gathering sitelinks, third-party links, and external identifier links. This will take a while ...')
    link_amount = 0
    url_pids = set()
    for result in sparql_queries.url_pids_query():
        url_pids.add(result)
    ext_id_pids_to_urls = defaultdict(set)
    for result in sparql_queries.external_id_pids_and_urls_query():
        for pid, formatter_url in result.items():
            ext_id_pids_to_urls[pid].add(formatter_url)
    # TODO run query to get ext IDs
    for result in api_requests.get_links(aggregated.keys(), url_pids, ext_id_pids_to_urls):
        for qid, url in result.items():
            if not aggregated[qid].get('links'):
                aggregated[qid]['links'] = set()
            aggregated[qid]['links'].add(url)
            link_amount += 1
    LOGGER.info('Got %d links', link_amount)
    LOGGER.info('Starting check against target links')
    for qid, data in aggregated.items():
        identifiers = data['identifiers']
        source_links = data.get('links')
        if not source_links:
            LOGGER.warning('Skipping check: no links available in QID %s', qid)
            continue
        for target_id in identifiers:
            if target_id in target_ids_to_links.keys():
                target_links = target_ids_to_links.get(target_id)
                if not target_links:
                    LOGGER.warning(
                        'Skipping check: no links available in external ID %s', target_id)
                    continue
                else:
                    target_links = set(target_links)
                shared_links = source_links.intersection(target_links)
                if not shared_links:
                    LOGGER.debug(
                        'No shared links between %s and %s. The identifier statement will be deprecated', qid, target_id)
                    deprecated[target_id].add(qid)
                else:
                    LOGGER.info('%s and %s share these links: %s',
                                qid, target_id, shared_links)
                    extra_links = target_links.difference(source_links)
                    if extra_links:
                        LOGGER.info(
                            '%s has extra links that will be added to %s: %s', target_id, qid, extra_links)
                        to_be_added[qid].update(extra_links)
                    else:
                        LOGGER.info('%s has no extra links', target_id)
    return {target_id: list(qids) for target_id, qids in deprecated.items()}, {qid: list(links) for qid, links in to_be_added.items()}


@click.command()
def check_metadata_cli():
    # TODO
    pass


def check_metadata():
    # TODO https://github.com/Wikidata/soweego/issues/89
    pass
