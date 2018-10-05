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

LOGGER = logging.getLogger(__name__)

WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'
BUCKET_SIZE = 50

WD = '/Users/focs/soweego/soweego/wikidata/resources/'
SAMPLE = 'imdb_unlinked_producers_sample'


def get_links(qids: set, url_pids: set, ext_id_pids_to_urls: dict) -> Iterator[dict]:
    """Get sitelinks and third-party links for each Wikidata item in the given set.

    :param qids: set of Wikidata QIDs
    :type qids: set
    :param url_pids: set of Wikidata PIDs having a URL as expected value
    :type url_pids: set
    :param ext_id_pids_to_urls: a dictionary ``{external_ID_PID: [formatter_URLs]}``
    :type ext_id_pids_to_urls: dict
    :return: a generator yielding ``{QID: URL}``
    :rtype: Iterator[dict]
    """
    qid_buckets = _make_buckets(qids)
    request_params = {
        'action': 'wbgetentities',
        'format': 'json',
        'props': 'sitelinks|claims'
    }
    no_sitelinks_count = 0
    no_links_count = 0
    no_ext_ids_count = 0
    for bucket in qid_buckets:
        request_params['ids'] = '|'.join(bucket)
        connection_is_ok = True
        while True:
            try:
                response = get(WIKIDATA_API_URL, params=request_params)
                log_request_data(response, LOGGER)
            except ChunkedEncodingError:
                LOGGER.error(
                    'Connection broken, will retry the request to the Wikidata API.')
                connection_is_ok = False
            else:
                connection_is_ok = True
            if connection_is_ok:
                break
        if response.ok:
            LOGGER.debug(
                'Successful %s to the Wikidata API. Status code: %d', response.request.method, response.status_code)
            response_body = response.json()
            for qid in response_body['entities']:
                entity = response_body['entities'][qid]
                sitelinks = entity.get('sitelinks')
                if sitelinks:
                    LOGGER.debug('Sitelinks for %s: %s', qid, sitelinks)
                    for site, data in sitelinks.items():
                        url = _build_sitelink_url(site, data['title'])
                        yield {qid: url}
                else:
                    LOGGER.debug('No sitelinks for %s', qid)
                    no_sitelinks_count += 1
                claims = entity.get('claims')
                if claims:
                    # Third-party URLs
                    available_url_pids = url_pids.intersection(claims.keys())
                    if available_url_pids:
                        LOGGER.debug(
                            'Available PIDs with URLs for %s: %s', qid, available_url_pids)
                        for pid in available_url_pids:
                            for pid_claim in claims[pid]:
                                url = _extract_value_from_claim(
                                    pid_claim, pid, qid)
                                if not url:
                                    continue
                                yield {qid: url}
                    else:
                        LOGGER.debug('No third-party links for %s', qid)
                        no_links_count += 1
                    # External IDs URLs
                    available_ext_id_pids = set(
                        ext_id_pids_to_urls.keys()).intersection(claims.keys())
                    if available_ext_id_pids:
                        LOGGER.debug(
                            'Available PIDs with external IDs for %s: %s', qid, available_ext_id_pids)
                        for pid in available_ext_id_pids:
                            for pid_claim in claims[pid]:
                                ext_id = _extract_value_from_claim(
                                    pid_claim, pid, qid)
                                if not ext_id:
                                    continue
                                for formatter_url in ext_id_pids_to_urls[pid]:
                                    yield {qid: formatter_url.replace('$1', ext_id)}
                    else:
                        LOGGER.debug(
                            'No external identifier links for %s', qid)
                        no_ext_ids_count += 1
                else:
                    LOGGER.warning('No claims for QID %s', qid)
        else:
            LOGGER.warning('Skipping failed %s to the Wikidata API. Reason: %d %s - Full URL: %s',
                           response.request.method, response.status_code, response.reason, response.request.url)
            continue
    LOGGER.info('Total QIDs with no sitelinks: %d', no_sitelinks_count)
    LOGGER.info('Total QIDs with no third-party links: %d', no_links_count)


def _extract_value_from_claim(pid_claim, pid, qid):
    LOGGER.debug('Processing (%s, %s) claim: %s', qid, pid, pid_claim)
    main_snak = pid_claim.get('mainsnak')
    if not main_snak:
        LOGGER.error(
            'Skipping malformed (%s, %s) claim with no main snak: %s', qid, pid, pid_claim)
        return None
    snak_type = main_snak.get('snaktype')
    if not snak_type:
        LOGGER.error(
            'Skipping malformed (%s, %s) claim with no snak type: %s', qid, pid, pid_claim)
        return None
    if snak_type == 'novalue':
        LOGGER.error(
            'Skipping malformed (%s, %s) claim with no value: %s', qid, pid, pid_claim)
        return None
    data_value = main_snak.get('datavalue')
    if not data_value:
        LOGGER.error(
            'Skipping malformed (%s, %s) claim with no datavalue: %s', qid, pid, pid_claim)
        return None
    value = data_value.get('value')
    if not value:
        LOGGER.error(
            'Skipping malformed (%s, %s) claim with no value: %s', qid, pid, pid_claim)
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
