#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of specific requests for the Wikidata API:
https://www.wikidata.org/w/api.php
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from typing import Iterator
from urllib.parse import urlunsplit

from requests import get
from requests.exceptions import ChunkedEncodingError

from soweego.commons.logging import log_request_data
from soweego.wikidata.vocabulary import LINKER_PIDS, METADATA_PIDS

LOGGER = logging.getLogger(__name__)

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
BUCKET_SIZE = 50


def get_data_for_linker(qids: set, url_pids: set, ext_id_pids_to_urls: dict) -> Iterator[tuple]:
    no_labels_count = 0
    no_aliases_count = 0
    no_descriptions_count = 0
    no_sitelinks_count = 0
    no_links_count = 0
    no_ext_ids_count = 0
    no_claims_count = 0

    qid_buckets, request_params = _prepare_request(
        qids, 'labels|aliases|descriptions|sitelinks|claims')
    for bucket in qid_buckets:
        response_body = _make_request(bucket, request_params)
        if not response_body:
            continue
        for qid in response_body['entities']:
            entity = response_body['entities'][qid]
            # Labels
            labels = entity.get('labels')
            if not labels:
                LOGGER.info('Skipping QID with no labels: %s', qid)
                no_labels_count += 1
                continue
            yield _yield_monolingual_strings(qid, labels, 'label')
            # Aliases
            aliases = entity.get('aliases')
            if not aliases:
                LOGGER.info('Skipping QID with no aliases: %s', qid)
                no_aliases_count += 1
                continue
            yield _yield_aliases(qid, aliases)
            # Descriptions
            descriptions = entity.get('descriptions')
            if not descriptions:
                LOGGER.info('Skipping QID with no descriptions: %s', qid)
                no_descriptions_count += 1
                continue
            yield _yield_monolingual_strings(qid, descriptions, 'description')
            # Sitelinks
            yield _yield_sitelinks(entity, qid, no_sitelinks_count)
            claims = entity.get('claims')
            if not claims:
                LOGGER.info('Skipping QID with no claims: %s', qid)
                no_claims_count += 1
                continue
            # Remember the following yield a generator of generators
            # see https://stackoverflow.com/questions/6503079/understanding-nested-yield-return-in-python#6503192
            # Third-party links
            yield _yield_expected_values(qid, claims, url_pids, no_links_count)
            # External ID links
            yield _yield_ext_id_links(ext_id_pids_to_urls,
                                      claims, qid, no_ext_ids_count)
            # Claims
            yield _yield_expected_values(qid, claims, set(LINKER_PIDS.keys()), no_claims_count, include_pid=True)

    LOGGER.info('QIDs: got %d with no labels, %d with no aliases, %d with no descriptions, %d with no sitelinks, %d with no third-party links, %d with no external ID links, %d with no expected claims',
                no_labels_count, no_aliases_count, no_descriptions_count, no_sitelinks_count, no_links_count, no_ext_ids_count, no_claims_count)


def get_metadata(qids: set) -> Iterator[tuple]:
    no_claims_count = 0

    qid_buckets, request_params = _prepare_request(qids, 'claims')
    for bucket in qid_buckets:
        response_body = _make_request(bucket, request_params)
        if not response_body:
            continue
        for qid in response_body['entities']:
            claims = response_body['entities'][qid].get('claims')
            if not claims:
                LOGGER.info('Skipping QID with no claims: %s', qid)
                no_claims_count += 1
                continue
            # Remember this yields a generator of generators
            # see https://stackoverflow.com/questions/6503079/understanding-nested-yield-return-in-python#6503192
            yield _yield_expected_values(qid, claims, METADATA_PIDS, no_claims_count, include_pid=True)

    LOGGER.info('Got %d QIDs with no %s claims',
                no_claims_count, METADATA_PIDS)


def get_links(qids: set, url_pids: set, ext_id_pids_to_urls: dict) -> Iterator[tuple]:
    """Get sitelinks and third-party links for each Wikidata item in the given set.

    :param qids: set of Wikidata QIDs
    :type qids: set
    :param url_pids: set of Wikidata PIDs having a URL as expected value
    :type url_pids: set
    :param ext_id_pids_to_urls: a dictionary ``{external_ID_PID: {formatter_URL: formatter_regex}}``
    :type ext_id_pids_to_urls: dict
    :return: a generator yielding ``QID, URL`` tuples
    :rtype: Iterator[tuple]
    """
    no_sitelinks_count = 0
    no_links_count = 0
    no_ext_ids_count = 0

    qid_buckets, request_params = _prepare_request(qids, 'sitelinks|claims')
    for bucket in qid_buckets:
        response_body = _make_request(bucket, request_params)
        if not response_body:
            continue
        for qid in response_body['entities']:
            entity = response_body['entities'][qid]
            # Sitelinks
            yield _yield_sitelinks(entity, qid, no_sitelinks_count)

            claims = entity.get('claims')
            if claims:
                # Third-party links
                yield _yield_expected_values(qid, claims, url_pids, no_links_count)
                # External ID links
                yield _yield_ext_id_links(ext_id_pids_to_urls,
                                          claims, qid, no_ext_ids_count)
            else:
                LOGGER.warning('No claims for QID %s', qid)

    LOGGER.info('QIDs: got %d with no sitelinks, %d with no third-party links, %d with no external ID links',
                no_sitelinks_count, no_links_count, no_ext_ids_count)


def _yield_monolingual_strings(qid, strings, string_type):
    for language_code, data in strings.items():
        string = data.get('value')
        if not string:
            LOGGER.warning(
                'Skipping malformed monolingual string with no value for %s: %s', qid, data)
            continue
        yield qid, language_code, string, string_type


def _yield_aliases(qid, aliases):
    for language_code, values in aliases.items():
        for data in values:
            alias = data.get('value')
            if not alias:
                LOGGER.warning(
                    'Skipping malformed alias with no value for %s: %s', qid, data)
                continue
            yield qid, language_code, alias, 'alias'


def _yield_sitelinks(entity, qid, no_sitelinks_count):
    sitelinks = entity.get('sitelinks')
    if not sitelinks:
        LOGGER.debug('No sitelinks for %s', qid)
        no_sitelinks_count += 1
    else:
        LOGGER.debug('Sitelinks for %s: %s', qid, sitelinks)
        for site, data in sitelinks.items():
            url = _build_sitelink_url(site, data['title'])
            yield qid, url


def _yield_ext_id_links(ext_id_pids_to_urls, claims, qid, no_ext_ids_count):
    available_ext_id_pids = set(
        ext_id_pids_to_urls.keys()).intersection(claims.keys())
    if not available_ext_id_pids:
        LOGGER.debug(
            'No external identifier links for %s', qid)
        no_ext_ids_count += 1
    else:
        LOGGER.debug(
            'Available PIDs with external IDs for %s: %s', qid, available_ext_id_pids)
        for pid in available_ext_id_pids:
            for pid_claim in claims[pid]:
                ext_id = _extract_value_from_claim(
                    pid_claim, pid, qid)
                if not ext_id:
                    continue
                for formatter_url in ext_id_pids_to_urls[pid]:
                    yield qid, formatter_url.replace('$1', ext_id)


def _yield_expected_values(qid, claims, expected_pids, count, include_pid=False):
    available = expected_pids.intersection(claims.keys())
    if not available:
        LOGGER.debug('No %s claims for %s', expected_pids, qid)
        count += 1
    else:
        LOGGER.debug(
            'Available claims for %s: %s', qid, available)
        for pid in available:
            for pid_claim in claims[pid]:
                value = _extract_value_from_claim(
                    pid_claim, pid, qid)
                if not value:
                    continue
                if include_pid:
                    yield qid, pid, value
                else:
                    yield qid, value


def _prepare_request(qids, props):
    qid_buckets = _make_buckets(qids)
    request_params = {
        'action': 'wbgetentities',
        'format': 'json',
        'props': props
    }
    return qid_buckets, request_params


def _make_request(bucket, params):
    params['ids'] = '|'.join(bucket)
    connection_is_ok = True
    while True:
        try:
            response = get(WIKIDATA_API_URL, params=params)
            log_request_data(response, LOGGER)
        except ChunkedEncodingError:
            LOGGER.warning(
                'Connection broken, retrying the request to the Wikidata API')
            connection_is_ok = False
        else:
            connection_is_ok = True
        if connection_is_ok:
            break
    if not response.ok:
        LOGGER.warning('Skipping failed %s to the Wikidata API. Reason: %d %s - Full URL: %s',
                       response.request.method, response.status_code, response.reason, response.request.url)
        return None
    LOGGER.debug(
        'Successful %s to the Wikidata API. Status code: %d', response.request.method, response.status_code)
    return response.json()


def _extract_value_from_claim(pid_claim, pid, qid):
    LOGGER.debug('Processing (%s, %s) claim: %s', qid, pid, pid_claim)
    main_snak = pid_claim.get('mainsnak')
    if not main_snak:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no main snak', qid, pid)
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    snak_type = main_snak.get('snaktype')
    if not snak_type:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no snak type', qid, pid)
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    if snak_type == 'novalue':
        LOGGER.warning(
            "Skipping unexpected (%s, %s) claim with 'novalue' snak type", qid, pid)
        LOGGER.debug(
            "Unexpected claim with 'novalue' snak type: %s", pid_claim)
        return None
    data_value = main_snak.get('datavalue')
    if not data_value:
        LOGGER.warning(
            "Skipping unexpected (%s, %s) claim with no 'datavalue'", qid, pid)
        LOGGER.debug("Unexpected claim with no 'datavalue': %s", pid_claim)
        return None
    value = data_value.get('value')
    if not value:
        LOGGER.warning(
            'Skipping malformed (%s, %s) claim with no value', qid, pid)
        LOGGER.debug('Malformed claim: %s', pid_claim)
        return None
    LOGGER.debug(
        'QID: %s - PID: %s - Value: %s', qid, pid, value)
    return value


def _build_sitelink_url(site, title):
    netloc_builder = []
    split_index = site.find('wiki')
    language = site[:split_index]
    netloc_builder.append(language.replace('_', '-'))
    project = site[split_index:]
    if project == 'wiki':
        project = 'wikipedia'
    if language == 'commons':
        project = 'wikimedia'
    netloc_builder.append(project)
    netloc_builder.append('org')
    url = urlunsplit(('https', '.'.join(netloc_builder),
                      '/wiki/%s' % title.replace(' ', '_'), '', ''))
    LOGGER.debug('Site: %s - Title: %s - Full URL: %s', site, title, url)
    return url


def _make_buckets(qids):
    buckets = []
    current_bucket = []
    for qid in qids:
        current_bucket.append(qid)
        if len(current_bucket) >= BUCKET_SIZE:
            buckets.append(current_bucket)
            current_bucket = []
    buckets.append(current_bucket)
    LOGGER.info('Made %d buckets of size %d out of %d QIDs to comply with the Wikidata API limits',
                len(buckets), BUCKET_SIZE, len(qids))
    return buckets
