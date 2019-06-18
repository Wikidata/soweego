#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of specific SPARQL queries for Wikidata data collection."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
import time
from csv import DictReader
from re import search
from typing import Generator, Set, Tuple, Iterator, Union, Dict

from requests import get

from soweego.commons import constants, keys
from soweego.commons.logging import log_request_data
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'
DEFAULT_RESPONSE_FORMAT = 'text/tab-separated-values'
JSON_RESPONSE_FORMAT = 'application/json'

ITEM_BINDING = '?item'
IDENTIFIER_BINDING = '?identifier'
PROPERTY_BINDING = '?property'
LINK_BINDING = '?link'
FORMATTER_URL_BINDING = '?formatter_url'
FORMATTER_REGEX_BINDING = '?formatter_regex'

URL_PID_TERMS = ' '.join(['wdt:%s' % pid for pid in vocabulary.URL_PIDS])

IDENTIFIER_QUERY_TEMPLATE = (
    'SELECT DISTINCT '
    + ITEM_BINDING
    + ' '
    + IDENTIFIER_BINDING
    + ' WHERE { '
    + ITEM_BINDING
    + ' wdt:%s/wdt:P279* wd:%s ; wdt:%s '
    + IDENTIFIER_BINDING
    + ' . }'
)
LINKS_QUERY_TEMPLATE = (
    'SELECT DISTINCT '
    + ITEM_BINDING
    + ' '
    + LINK_BINDING
    + ' WHERE { VALUES '
    + PROPERTY_BINDING
    + ' { '
    + URL_PID_TERMS
    + ' } . '
    + ITEM_BINDING
    + ' wdt:%s/wdt:P279* wd:%s ; wdt:%s '
    + IDENTIFIER_BINDING
    + ' ; '
    + PROPERTY_BINDING
    + ' '
    + LINK_BINDING
    + ' . }'
)
DATASET_QUERY_TEMPLATE = (
    'SELECT DISTINCT '
    + ITEM_BINDING
    + ' WHERE { '
    + ITEM_BINDING
    + ' wdt:%s/wdt:P279* wd:%s . FILTER NOT EXISTS { '
    + ITEM_BINDING
    + ' wdt:%s '
    + IDENTIFIER_BINDING
    + ' . } . }'
)

VALUES_QUERY_TEMPLATE = (
    'SELECT * WHERE { VALUES '
    + ITEM_BINDING
    + ' { %s } . '
    + ITEM_BINDING
    + ' %s }'
)
CATALOG_QID_QUERY_TEMPLATE = (
    'SELECT '
    + ITEM_BINDING
    + ' WHERE { wd:%s wdt:P1629 '
    + ITEM_BINDING
    + ' . }'
)

PROPERTIES_WITH_URL_DATATYPE_QUERY = (
    'SELECT '
    + PROPERTY_BINDING
    + ' WHERE { '
    + PROPERTY_BINDING
    + ' a wikibase:Property ; wikibase:propertyType wikibase:Url . }'
)
URL_PIDS_QUERY = 'SELECT ?property WHERE { ?property a wikibase:Property ; wikibase:propertyType wikibase:Url . }'
EXT_ID_PIDS_AND_URLS_QUERY = (
    'SELECT * WHERE { '
    + PROPERTY_BINDING
    + ' a wikibase:Property ; wikibase:propertyType wikibase:ExternalId ; wdt:P1630 '
    + FORMATTER_URL_BINDING
    + ' . OPTIONAL { '
    + PROPERTY_BINDING
    + ' wdt:P1793 '
    + FORMATTER_REGEX_BINDING
    + ' . } . }'
)

SUBCLASSES_OF_QID = (
    'SELECT DISTINCT '
    + ITEM_BINDING
    + ' WHERE { '
    + ITEM_BINDING
    + ' wdt:P279* wd:%s . }'
)

SUPERCLASSES_OF_QID = (
    'SELECT DISTINCT '
    + ITEM_BINDING
    + ' WHERE { '
    + ' wd:%s wdt:P279* '
    + ITEM_BINDING
    + ' . }'
)


def external_id_pids_and_urls() -> Iterator[Dict]:
    """Retrieve Wikidata properties holding identifier values,
    together with their formatter URLs and regular expressions.

    :return: the generator yielding
      ``{PID: {formatter_URL: formatter_regex} }`` dicts
    """
    LOGGER.info(
        'Retrieving PIDs with external ID values, their formatter URLs and regexps'
    )
    result_set = _make_request(
        EXT_ID_PIDS_AND_URLS_QUERY, response_format=JSON_RESPONSE_FORMAT
    )
    for result in result_set['results']['bindings']:
        formatter_url_dict = result.get(FORMATTER_URL_BINDING.lstrip('?'))
        if not formatter_url_dict:
            LOGGER.warning(
                'Skipping malformed query result: no formatter URL binding in %s',
                result,
            )
            continue
        formatter_url = formatter_url_dict.get('value')
        if not formatter_url:
            LOGGER.warning(
                'Skipping malformed query result: no formatter URL in %s',
                formatter_url_dict,
            )
            continue
        formatter_regex_dict = result.get(FORMATTER_REGEX_BINDING.lstrip('?'))
        if formatter_regex_dict:
            formatter_regex = formatter_regex_dict.get('value')
            if not formatter_regex:
                LOGGER.warning(
                    'Skipping malformed query result: no formatter regex in %s',
                    formatter_regex_dict,
                )
                continue
        else:
            formatter_regex = None
            LOGGER.debug('No formatter regex in %s', result)
        pid_uri_dict = result.get(PROPERTY_BINDING.lstrip('?'))
        if not pid_uri_dict:
            LOGGER.warning(
                'Skipping malformed query result: no Wikidata property binding in %s',
                result,
            )
            continue
        pid_uri = pid_uri_dict.get('value')
        if not pid_uri:
            LOGGER.warning(
                'Skipping malformed query result: no Wikidata property in %s',
                pid_uri_dict,
            )
            continue
        pid = search(constants.PID_REGEX, pid_uri)
        if not pid:
            LOGGER.warning(
                'Skipping malformed query result: invalid Wikidata property URI %s in %s',
                pid_uri,
                result,
            )
            continue
        yield {pid.group(): {formatter_url: formatter_regex}}


def run_query(
    query_type: Tuple[str, str], class_qid: str, catalog_pid: str, result_per_page: int
) -> Iterator[Union[Tuple, str]]:
    """Run a filled SPARQL query template against the Wikidata endpoint
    with eventual paging.

    :param query_type: a pair with one of
      ``{'identifier', 'links', 'dataset', 'biodata'}`` and
      ``{'class', 'occupation'}``
    :param class_qid: a Wikidata ontology class,
      like `Q5 <https://www.wikidata.org/wiki/Q5>`_
    :param catalog_pid: a Wikidata property for identifiers,
      like `P1953 <https://www.wikidata.org/wiki/Property:P1953>`_
    :param result_per_page: a page size. Use ``0`` to switch paging off
    :return: the query result generator, yielding
      ``(QID, identifier_or_URL)`` pairs, or
      ``QID`` strings only, depending on *query_type*
    """
    what, how = query_type

    if how not in constants.SUPPORTED_QUERY_TYPES:
        LOGGER.critical(
            'Bad query type: %s. It should be one of %s',
            how,
            constants.SUPPORTED_QUERY_TYPES,
        )
        raise ValueError(
            'Bad query type: %s. It should be one of %s'
            % (how, constants.SUPPORTED_QUERY_TYPES)
        )

    if what == keys.IDENTIFIER:
        query = (
            IDENTIFIER_QUERY_TEMPLATE
            % (vocabulary.INSTANCE_OF, class_qid, catalog_pid)
            if how == keys.CLASS_QUERY
            else IDENTIFIER_QUERY_TEMPLATE
            % (vocabulary.OCCUPATION, class_qid, catalog_pid)
        )
        return _parse_query_result(
            keys.IDENTIFIER, _run_paged_query(result_per_page, query)
        )
    elif what == keys.LINKS:
        query = (
            LINKS_QUERY_TEMPLATE
            % (vocabulary.INSTANCE_OF, class_qid, catalog_pid)
            if how == keys.CLASS_QUERY
            else LINKS_QUERY_TEMPLATE
            % (vocabulary.OCCUPATION, class_qid, catalog_pid)
        )
        return _parse_query_result(
            keys.LINKS, _run_paged_query(result_per_page, query)
        )
    elif what == keys.DATASET:
        query = (
            DATASET_QUERY_TEMPLATE
            % (vocabulary.INSTANCE_OF, class_qid, catalog_pid)
            if how == keys.CLASS_QUERY
            else DATASET_QUERY_TEMPLATE
            % (vocabulary.OCCUPATION, class_qid, catalog_pid)
        )
        return _parse_query_result(
            keys.DATASET, _run_paged_query(result_per_page, query)
        )
    elif what == keys.BIODATA:
        # TODO implement metadata query to improve the validator
        raise NotImplementedError
    else:
        LOGGER.critical(
            'Bad query type: %s. It should be one of %s',
            what,
            constants.SUPPORTED_QUERY_SELECTORS,
        )
        raise ValueError(
            'Bad query type: %s. It should be one of %s'
            % (what, constants.SUPPORTED_QUERY_SELECTORS)
        )


def subclasses_of(qid: str) -> Set[str]:
    """Retrieve subclasses of a given Wikidata ontology class.

    :param qid: a Wikidata ontology class,
      like `Q5 <https://www.wikidata.org/wiki/Q5>`_
    :return: the QIDs of subclasses
    """

    # subclasses (?res is subclass of QID)
    result = _make_request(SUBCLASSES_OF_QID % qid)

    return set(_get_valid_qid(item).group() for item in result)


def superclasses_of(qid: str) -> Set[str]:
    """Retrieve superclasses of a given Wikidata ontology class.

    :param qid: a Wikidata ontology class,
      like `Q5 <https://www.wikidata.org/wiki/Q5>`_
    :return: the QIDs of superclasses
    """

    # superclasses (QID is subclass or ?res)
    result = _make_request(SUPERCLASSES_OF_QID % qid)

    return set(_get_valid_qid(item).group() for item in result)


def url_pids() -> Iterator[str]:
    """Retrieve Wikidata properties holding URL values.

    :return: the PIDs generator
    """
    LOGGER.info('Retrieving PIDs with URL values')
    result_set = _make_request(URL_PIDS_QUERY)
    for result in result_set:
        valid_pid = _get_valid_pid(result)
        if not valid_pid:
            continue
        yield valid_pid.group()


def _get_valid_pid(result):
    pid_uri = result.get(PROPERTY_BINDING)
    if not pid_uri:
        LOGGER.warning(
            'Skipping malformed query result: no Wikidata property in %s',
            result,
        )
        return None
    pid = search(constants.PID_REGEX, pid_uri)
    if not pid:
        LOGGER.warning(
            'Skipping malformed query result: invalid Wikidata property URI %s in %s',
            pid_uri,
            result,
        )
        return None
    return pid


def _get_valid_qid(result):
    item_uri = result.get(ITEM_BINDING)
    if not item_uri:
        LOGGER.warning(
            'Skipping malformed query result: no Wikidata item in %s', result
        )
        return None
    qid = search(constants.QID_REGEX, item_uri)
    if not qid:
        LOGGER.warning(
            'Skipping malformed query result: invalid Wikidata item URI %s in %s',
            item_uri,
            result,
        )
        return None
    return qid


def _make_request(query, response_format=DEFAULT_RESPONSE_FORMAT):
    response = get(
        WIKIDATA_SPARQL_ENDPOINT,
        params={'query': query},
        headers={
            'Accept': response_format,
            'User-Agent': constants.HTTP_USER_AGENT,
        },
    )
    log_request_data(response, LOGGER)

    if response.ok:
        LOGGER.debug(
            'Successful GET to the Wikidata SPARQL endpoint. Status code: %d',
            response.status_code,
        )
        if response_format == JSON_RESPONSE_FORMAT:
            LOGGER.debug('Returning JSON results')
            return response.json()
        response_body = response.text.splitlines()
        if len(response_body) == 1:
            LOGGER.debug('Got an empty result set from query: %s', query)
            return 'empty'
        LOGGER.debug('Got %d results', len(response_body) - 1)
        return DictReader(response_body, delimiter='\t')

    if response.status_code == 429:
        # according to the maintainer: https://stackoverflow.com/a/42590757/2234619
        # and here: https://github.com/wikimedia/puppet/blob/837d10e240932b8042b81acf31a8808f603b08bb/modules/wdqs/templates/nginx.erb#L85
        # the only limit on the SPARQL API is that there can't be more than 5 concurrent requests per IP

        # so if the status code is 429 then we've made too many requests we just wait a bit
        # and retry

        # arbitrary wait time (seconds)
        wait_time = 0.3

        LOGGER.warning(
            'Exceeded request API request limit. '
            'Will retry after %s seconds',
            wait_time,
        )

        # block the current thread for `wait_time`
        time.sleep(wait_time)

        # retry the request and return result
        return _make_request(query, response_format)

    LOGGER.warning(
        'The GET to the Wikidata SPARQL endpoint went wrong. Reason: %d %s - Query: %s',
        response.status_code,
        response.reason,
        query,
    )
    return None


def _parse_query_result(query_type, result_set):
    # Paranoid checks for malformed results:
    # it should never happen, but it actually does
    identifier_or_link = False
    for result in result_set:
        if query_type == keys.IDENTIFIER:
            identifier_or_link = result.get(IDENTIFIER_BINDING)
            to_be_logged = 'external identifier'
        elif query_type == keys.LINKS:
            identifier_or_link = result.get(LINK_BINDING)
            to_be_logged = 'third-party URL'

        if identifier_or_link is None:
            LOGGER.warning(
                'Skipping malformed query result: no %s in %s',
                to_be_logged,
                result,
            )
            continue

        valid_qid = _get_valid_qid(result)
        if not valid_qid:
            continue

        if query_type == keys.DATASET:
            yield valid_qid.group()
        else:
            yield valid_qid.group(), identifier_or_link


def _run_paged_query(result_per_page, query):
    if result_per_page == 0:
        LOGGER.info('Running query without paging: %s', query)
        result_set = _make_request(query)
        if not result_set:
            LOGGER.error('The query went wrong')
            yield {}
        if result_set == 'empty':
            LOGGER.warning('Empty result')
            yield {}
        for result in result_set:
            yield result
    else:
        LOGGER.info(
            'Running paged query with %d results per page: %s',
            result_per_page,
            query,
        )
        pages = 0
        while True:
            LOGGER.info('Page #%d', pages)
            query_builder = [query]
            query_builder.append(
                'OFFSET %d LIMIT %d'
                % (result_per_page * pages, result_per_page)
            )
            result_set = _make_request(' '.join(query_builder))
            if not result_set:
                LOGGER.error(
                    'Skipping page %d because the query went wrong', pages
                )
                pages += 1
                continue
            if result_set == 'empty':
                LOGGER.info('Paging finished. Total pages: %d', pages)
                break
            for result in result_set:
                yield result
            pages += 1
