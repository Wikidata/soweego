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
from functools import lru_cache
from pkgutil import get_data

import click
from soweego.commons.db_manager import DBManager
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.musicbrainz_entity import (MusicbrainzBandEntity,
                                                        MusicbrainzPersonEntity)
from soweego.wikidata import api_requests, sparql_queries, vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('wikidata_query_type', type=click.Choice(['class', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('database_table')
@click.option('-o', '--outfile', type=click.File('w'), default='output/non_existent_ids.json', help="default: 'output/non_existent_ids.json'")
def check_existence_cli(wikidata_query_type, class_qid, catalog_pid, database_table, outfile):
    """Check the existence of identifier statements.

    Dump a JSON file of invalid ones ``{identifier: QID}``
    """
    entity = BaseEntity
    if database_table == 'musicbrainz_person':
        entity = MusicbrainzPersonEntity
    elif database_table == 'musicbrainz_band':
        entity = MusicbrainzBandEntity
    else:
        LOGGER.error('Not able to retrive entity for given database_table')

    invalid = check_existence(wikidata_query_type, class_qid,
                              catalog_pid, entity)
    json.dump(invalid, outfile, indent=2)


def check_existence(class_or_occupation_query, class_qid, catalog_pid, entity: BaseEntity):
    query_type = 'identifier', class_or_occupation_query

    db_manager = DBManager()
    session = db_manager.new_session()

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
            clean_parts = _clean(url)
            LOGGER.debug('Clean URL: %s', clean_parts)
            for part in clean_parts:
                valid_url = _validate(part)
                if not valid_url:
                    continue
                LOGGER.debug('Valid URL: <%s>', valid_url)
                resolved = _resolve(valid_url)
                if not resolved:
                    dead_urls += 1
                    continue
                LOGGER.debug('Living URL: <%s>', resolved)
                ext_id, pid = _get_ext_id_from_url(
                    resolved, ext_id_pids_to_urls)
                if ext_id:
                    ext_ids_to_be_added.append((qid, pid, ext_id))
                else:
                    urls_to_be_added.append(
                        (qid, vocabulary.DESCRIBED_AT_URL_PID, resolved))
    LOGGER.info('Check completed. %d statements to be deprecated, %d external IDs to be added, %d URL statements to be added, %d dead URLs', len(
        deprecated), len(ext_ids_to_be_added), len(urls_to_be_added), dead_urls)
    return {target_id: list(qids) for target_id, qids in deprecated.items()}, ext_ids_to_be_added, urls_to_be_added


# TODO run this function in the ingestor handler (during dump extraction)
def _clean(url):
    stripped = url.strip()
    if ' ' in stripped:
        items = stripped.split()
        LOGGER.info('URL <%s> split into %s', url, items)
        return items
    return [stripped]


# Adapted from https://github.com/django/django/blob/master/django/core/validators.py
# See DJANGO_LICENSE
def _validate(url):
    ul = '\u00a1-\uffff'  # Unicode letters range (must not be a raw string)
    # IP patterns
    ipv4_re = r'(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}'
    ipv6_re = r'\[[0-9a-f:\.]+\]'
    # Host patterns
    hostname_re = r'[a-z' + ul + \
        r'0-9](?:[a-z' + ul + r'0-9-]{0,61}[a-z' + ul + r'0-9])?'
    # Max length for domain name labels is 63 characters per RFC 1034 sec. 3.1
    domain_re = r'(?:\.(?!-)[a-z' + ul + r'0-9-]{1,63}(?<!-))*'
    # Top-level domain pattern
    tld_re = (
        r'\.'                                # Dot
        r'(?!-)'                             # Can't start with a dash
        r'(?:[a-z' + ul + '-]{2,63}'         # Domain label
        r'|xn--[a-z0-9]{1,59})'              # Or punycode label
        r'(?<!-)'                            # Can't end with a dash
        r'\.?'                               # May have a trailing dot
    )
    host_re = '(' + hostname_re + domain_re + tld_re + '|localhost)'
    final_regex = re.compile(
        r'^((?:[a-z0-9\.\-\+]*)://)?'  # Scheme is optional
        r'(?:[^\s:@/]+(?::[^\s:@/]*)?@)?'  # user:pass authentication
        r'(?:' + ipv4_re + '|' + ipv6_re + '|' + host_re + ')'
        r'(?::\d{2,5})?'  # Port
        r'(?:[/?#][^\s]*)?'  # Resource path
        r'\Z', re.IGNORECASE)
    valid_url = re.search(final_regex, url)
    if not valid_url:
        LOGGER.info('Dropping invalid URL: <%s>', url)
        return None
    if not valid_url.group(1):
        LOGGER.warning(
            "Adding 'https' to potential URL with missing scheme: <%s>", url)
        return 'https://' + valid_url.group()
    return valid_url.group()


@lru_cache()
def _resolve(url):
    # Don't show warnings in case of unverified HTTPS requests
    disable_warnings(InsecureRequestWarning)
    # Some Web sites return 4xx just because of a non-browser user agent header
    browser_ua = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:62.0) Gecko/20100101 Firefox/62.0'}
    try:
        # Some Web sites do not accept the HEAD method: fire a GET, but don't download anything
        response = get(url, headers=browser_ua, stream=True)
    except requests.exceptions.SSLError as ssl_error:
        LOGGER.debug(
            'SSL certificate verification failed, will retry without verification. Original URL: <%s> - Reason: %s', url, ssl_error)
        try:
            response = get(url, headers=browser_ua, stream=True, verify=False)
        except Exception as unexpected_error:
            LOGGER.warning(
                'Dropping URL that led to an unexpected error: <%s> - Reason: %s', url, unexpected_error)
            return None
    except requests.exceptions.ConnectionError as connection_error:
        LOGGER.info(
            'Dropping URL that led to an aborted connection: <%s> - Reason: %s', url, connection_error)
        return None
    except requests.exceptions.TooManyRedirects as too_many_redirects:
        LOGGER.info(
            'Dropping URL because of too many redirects: <%s> - %s', url, too_many_redirects)
    except Exception as unexpected_error:
        LOGGER.warning(
            'Dropping URL that led to an unexpected error: <%s> - Reason: %s', url, unexpected_error)
        return None
    if not response.ok:
        LOGGER.info(
            "Dropping dead URL that returned HTTP status '%s' (%d): <%s>", response.reason, response.status_code, url)
        return None
    resolved = response.url
    history = response.history
    if len(history) > 1:
        LOGGER.debug('Resolution chain from original URL to resolved URL: %s', [
                     r.url for r in history])
    else:
        LOGGER.debug('Original URL: <%s> - Resolved URL: <%s>', url, resolved)
    return resolved


def _get_ext_id_from_url(url, ext_id_pids_to_urls):
    LOGGER.debug('Trying to extract an identifier from URL <%s>', url)
    for pid, formatters in ext_id_pids_to_urls.items():
        for formatter_url, formatter_regex in formatters.items():
            before, _, after = formatter_url.partition('$1')
            if url.startswith(before) and url.endswith(after):
                LOGGER.debug(
                    'Input URL matches external ID formatter URL: <%s> -> <%s>', url, formatter_url)
                url_fragment = url[len(before):-len(after)
                                   ] if len(after) else url[len(before):]
                if not formatter_regex:
                    LOGGER.debug(
                        'Missing formatter regex, will assume the URL substring as the ID. URL: %s - URL substring: %s', url, url_fragment)
                    return url_fragment, pid
                ext_id_match = regex.search(formatter_regex, url_fragment) if isinstance(
                    formatter_regex, regex.Pattern) else re.search(formatter_regex, url_fragment)
                if not ext_id_match:
                    LOGGER.debug(
                        "Skipping target URL <%s> with fragment '%s' not matching the expected formatter regex %s", url, url_fragment, formatter_regex.pattern)
                    return None, None
                ext_id = ext_id_match.group()
                LOGGER.debug('URL: %s - URL substring: %s - formatter regex: %s - extracted ID: %s',
                             url, url_fragment, formatter_regex, ext_id)
                return ext_id, pid
    LOGGER.debug('Could not extract any identifier from URL <%s>', url)
    return None, None


@click.command()
def check_metadata_cli():
    # TODO https://github.com/Wikidata/soweego/issues/89
    pass


def check_metadata():
    # TODO https://github.com/Wikidata/soweego/issues/89
    pass
