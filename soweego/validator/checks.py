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
import re
from collections import defaultdict

import click
import regex

from soweego.commons import url_utils
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
@click.option('-e', '--ext-ids-outfile', type=click.File('w'), default='output/external_ids_to_be_added.tsv', help="default: 'output/external_ids_to_be_added.tsv'")
@click.option('-u', '--urls-outfile', type=click.File('w'), default='output/urls_to_be_added.tsv', help="default: 'output/urls_to_be_added.tsv'")
def check_links_cli(class_or_occupation_query, class_qid, catalog_pid, target_links, deprecated_outfile, ext_ids_outfile, urls_outfile):
    """Check the validity of identifier statements based on the available links.

    Dump 2 JSON files: deprecated identifiers ``{identifer: QID}``
    and links to be added ``{QID: [links]}``
    """
    loaded_target_links = json.load(target_links)
    LOGGER.info("Loaded target links file '%s'", target_links.name)
    deprecated, ext_ids_to_be_added, urls_to_be_added = check_links(
        class_or_occupation_query, class_qid, catalog_pid, loaded_target_links)
    json.dump(deprecated, deprecated_outfile, indent=2)
    ext_ids_outfile.writelines(
        ['\t'.join(triple) + '\n' for triple in ext_ids_to_be_added])
    urls_outfile.writelines(
        ['\t'.join(triple) + '\n' for triple in urls_to_be_added])
    LOGGER.info('Result dumped to %s, %s, %s', deprecated_outfile.name,
                ext_ids_outfile.name, urls_outfile.name)


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
    ext_id_pids_to_urls = defaultdict(dict)
    for result in sparql_queries.external_id_pids_and_urls_query():
        for pid, formatters in result.items():
            for formatter_url, formatter_regex in formatters.items():
                if formatter_regex:
                    try:
                        compiled_regex = re.compile(formatter_regex)
                    except re.error:
                        LOGGER.warning(
                            "Using 'regex' third-party library. Formatter regex not supported by the 're' standard library: %s", formatter_regex)
                        compiled_regex = regex.compile(formatter_regex)
                else:
                    compiled_regex = None
                ext_id_pids_to_urls[pid][formatter_url] = compiled_regex
    for result in api_requests.get_links(aggregated.keys(), url_pids, ext_id_pids_to_urls):
        for qid, url in result.items():
            if not aggregated[qid].get('links'):
                aggregated[qid]['links'] = set()
            aggregated[qid]['links'].add(url)
            link_amount += 1
    LOGGER.info('Got %d links', link_amount)
    LOGGER.info('Starting check against target links ...')
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
                        'Skipping check: no links available in target ID %s', target_id)
                    continue
                else:
                    target_links = set(target_links)
                shared_links = source_links.intersection(target_links)
                if not shared_links:
                    LOGGER.debug(
                        'No shared links between %s and %s. The identifier statement will be deprecated', qid, target_id)
                    deprecated[target_id].add(qid)
                else:
                    LOGGER.debug('%s and %s share these links: %s',
                                 qid, target_id, shared_links)
                    extra_links = target_links.difference(source_links)
                    if extra_links:
                        LOGGER.debug(
                            '%s has extra links that will be added to %s: %s', target_id, qid, extra_links)
                        to_be_added[qid].update(extra_links)
                    else:
                        LOGGER.debug('%s has no extra links', target_id)
    LOGGER.info('Starting consolidation of target links to be added ...')
    ext_ids_to_be_added = []
    urls_to_be_added = []
    dead_urls = 0
    for qid, urls in to_be_added.items():
        for url in urls:
            LOGGER.debug('Processing URL <%s>', url)
            clean_parts = url_utils.clean(url)
            LOGGER.debug('Clean URL: %s', clean_parts)
            for part in clean_parts:
                valid_url = url_utils.validate(part)
                if not valid_url:
                    continue
                LOGGER.debug('Valid URL: <%s>', valid_url)
                resolved = url_utils.resolve(valid_url)
                if not resolved:
                    dead_urls += 1
                    continue
                LOGGER.debug('Living URL: <%s>', resolved)
                ext_id, pid = url_utils.get_external_id_from_url(
                    resolved, ext_id_pids_to_urls)
                if ext_id:
                    ext_ids_to_be_added.append((qid, pid, ext_id))
                else:
                    urls_to_be_added.append(
                        (qid, vocabulary.DESCRIBED_AT_URL_PID, resolved))
    LOGGER.info('Check completed. %d statements to be deprecated, %d external IDs to be added, %d URL statements to be added, %d dead URLs', len(
        deprecated), len(ext_ids_to_be_added), len(urls_to_be_added), dead_urls)
    return {target_id: list(qids) for target_id, qids in deprecated.items()}, ext_ids_to_be_added, urls_to_be_added


@click.command()
def check_metadata_cli():
    # TODO https://github.com/Wikidata/soweego/issues/89
    pass


def check_metadata():
    # TODO https://github.com/Wikidata/soweego/issues/89
    pass
