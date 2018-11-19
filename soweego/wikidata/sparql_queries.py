#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of specific SPARQL queries for the Wikidata SPARQL endpoint:
https://query.wikidata.org/sparql
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
import os
from csv import DictReader
from functools import lru_cache
from re import search
from typing import Iterator

import click
from requests import get

from soweego.commons.logging import log_request_data
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

ITEM_REGEX = r'Q\d+'
PID_REGEX = r'P\d+'

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

IDENTIFIER_CLASS_BASED_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + IDENTIFIER_BINDING + \
    ' WHERE { ' + ITEM_BINDING + \
    ' wdt:' + vocabulary.INSTANCE_OF + \
    '/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + ' . }'
IDENTIFIER_OCCUPATION_BASED_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + IDENTIFIER_BINDING + \
    ' WHERE { ' + ITEM_BINDING + \
    ' wdt:' + vocabulary.OCCUPATION + \
    '/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + ' . }'
VALUES_QUERY_TEMPLATE = 'SELECT * WHERE { VALUES ' + \
    ITEM_BINDING + ' { %s } . ' + ITEM_BINDING + ' %s }'
PROPERTIES_WITH_URL_DATATYPE_QUERY = 'SELECT ' + PROPERTY_BINDING + \
    ' WHERE { ' + PROPERTY_BINDING + \
    ' a wikibase:Property ; wikibase:propertyType wikibase:Url . }'
LINKS_CLASS_BASED_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + LINK_BINDING + \
    ' WHERE { VALUES ' + PROPERTY_BINDING + ' { ' + URL_PID_TERMS + ' } . ' + ITEM_BINDING + ' wdt:' + vocabulary.INSTANCE_OF + '/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + \
    ' ; ' + PROPERTY_BINDING + ' ' + LINK_BINDING + ' . }'
LINKS_OCCUPATION_BASED_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + LINK_BINDING + \
    ' WHERE { VALUES ' + PROPERTY_BINDING + ' { ' + URL_PID_TERMS + ' } . ' + ITEM_BINDING + ' wdt:' + vocabulary.OCCUPATION + '/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + \
    ' ; ' + PROPERTY_BINDING + ' ' + LINK_BINDING + ' . }'
CATALOG_QID_QUERY_TEMPLATE = 'SELECT ' + ITEM_BINDING + \
    ' WHERE { wd:%s wdt:P1629 ' + ITEM_BINDING + ' . }'

URL_PIDS_QUERY = 'SELECT ?property WHERE { ?property a wikibase:Property ; wikibase:propertyType wikibase:Url . }'
EXT_ID_PIDS_AND_URLS_QUERY = 'SELECT * WHERE { ' + PROPERTY_BINDING + \
    ' a wikibase:Property ; wikibase:propertyType wikibase:ExternalId ; wdt:P1630 ' + \
    FORMATTER_URL_BINDING + \
    ' . OPTIONAL { ' + PROPERTY_BINDING + ' wdt:P1793 ' + \
    FORMATTER_REGEX_BINDING + ' . } . }'


@click.command()
@click.argument('ontology_class')
@click.argument('identifier_property')
@click.option('-p', '--results-per-page', default=1000, help='Default: 1000.')
@click.option('-o', '--outdir', type=click.Path(), default='output', help="Default: 'output'.")
def identifier_class_based_query_cli(ontology_class, identifier_property, results_per_page, outdir):
    """Run a paged SPARQL query against the Wikidata endpoint to get items and external catalog
    identifiers. Dump the result into a JSONlines file.

    IDENTIFIER_PROPERTY must be a Wikidata property identifier like 'P1953' (Discogs artist ID).

    ONTOLOGY_CLASS must be a Wikidata ontology class like 'Q5' (human).

    Use '-p 0' to switch paging off.
    """
    with open(os.path.join(outdir, 'class_based_identifier_query_result.jsonl'), 'w', 1) as outfile:
        _dump_result(identifier_class_based_query(
            ontology_class, identifier_property, results_per_page), outfile)
        LOGGER.info(
            "Class-based identifier query result dumped as JSON lines to '%s'", outfile.name)


@lru_cache()
def run_identifier_or_links_query(query_type: tuple, class_qid: str, catalog_pid: str, result_per_page: int) -> Iterator[dict]:
    """Run a filled SPARQL query template against the Wikidata endpoint with eventual paging.

    :param query_type: pair with one of ``identifier``, ``links``, ``metadata``, and either ``occupation`` or ``class``
    "type query_type: tuple
    :param class_qid: Wikidata ontology class like ``Q5`` (human)
    :type class_qid: str
    :param catalog_pid: Wikidata property for identifiers, like ``P1953`` (Discogs artist ID)
    :type catalog_pid: str
    :param result_per_page: page size. Use ``0`` to switch paging off
    :type result_per_page: int
    :return: query result generator yielding ``{QID: identifier_or_URL}``
    :rtype: Iterator[dict]
    """
    if query_type[0] == 'identifier':
        if query_type[1] == 'class':
            query_template = IDENTIFIER_CLASS_BASED_QUERY_TEMPLATE
        elif query_type[1] == 'occupation':
            query_template = IDENTIFIER_OCCUPATION_BASED_QUERY_TEMPLATE
        query = query_template % (class_qid, catalog_pid)
        return _parse_query_result('identifier', _run_paged_query(result_per_page, query))
    elif query_type[0] == 'links':
        if query_type[1] == 'class':
            query_template = LINKS_CLASS_BASED_QUERY_TEMPLATE
        elif query_type[1] == 'occupation':
            query_template = LINKS_OCCUPATION_BASED_QUERY_TEMPLATE
        query = query_template % (class_qid, catalog_pid)
        return _parse_query_result('links', _run_paged_query(result_per_page, query))
    elif query_type[0] == 'metadata':
        # TODO
        raise NotImplementedError


def catalog_qid_query(catalog_pid):
    LOGGER.info('Retrieving the catalog QID from PID %s', catalog_pid)
    result_set = make_request(CATALOG_QID_QUERY_TEMPLATE % catalog_pid)
    for result in result_set:
        valid_qid = _get_valid_qid(result)
        if not valid_qid:
            continue
        yield valid_qid.group()


@lru_cache()
def url_pids_query():
    LOGGER.info('Retrieving PIDs with URL values')
    result_set = make_request(URL_PIDS_QUERY)
    for result in result_set:
        valid_pid = _get_valid_pid(result)
        if not valid_pid:
            continue
        yield valid_pid.group()


@lru_cache()
def external_id_pids_and_urls_query():
    LOGGER.info(
        'Retrieving PIDs with external ID values, their formatter URLs and regexps')
    result_set = make_request(
        EXT_ID_PIDS_AND_URLS_QUERY, response_format=JSON_RESPONSE_FORMAT)
    for result in result_set['results']['bindings']:
        formatter_url_dict = result.get(FORMATTER_URL_BINDING.lstrip('?'))
        if not formatter_url_dict:
            LOGGER.warning(
                'Skipping malformed query result: no formatter URL binding in %s', result)
            continue
        formatter_url = formatter_url_dict.get('value')
        if not formatter_url:
            LOGGER.warning(
                'Skipping malformed query result: no formatter URL in %s', formatter_url_dict)
            continue
        formatter_regex_dict = result.get(FORMATTER_REGEX_BINDING.lstrip('?'))
        if formatter_regex_dict:
            formatter_regex = formatter_regex_dict.get('value')
            if not formatter_regex:
                LOGGER.warning(
                    'Skipping malformed query result: no formatter regex in %s', formatter_regex_dict)
                continue
        else:
            formatter_regex = None
            LOGGER.debug(
                'No formatter regex in %s', result)
        pid_uri_dict = result.get(PROPERTY_BINDING.lstrip('?'))
        if not pid_uri_dict:
            LOGGER.warning(
                'Skipping malformed query result: no Wikidata property binding in %s', result)
            continue
        pid_uri = pid_uri_dict.get('value')
        if not pid_uri:
            LOGGER.warning(
                'Skipping malformed query result: no Wikidata property in %s', pid_uri_dict)
            continue
        pid = search(PID_REGEX, pid_uri)
        if not pid:
            LOGGER.warning(
                'Skipping malformed query result: invalid Wikidata property URI %s in %s', pid_uri, result)
            continue
        yield {pid.group(): {formatter_url: formatter_regex}}


def _get_valid_pid(result):
    pid_uri = result.get(PROPERTY_BINDING)
    if not pid_uri:
        LOGGER.warning(
            'Skipping malformed query result: no Wikidata property in %s', result)
        return None
    pid = search(PID_REGEX, pid_uri)
    if not pid:
        LOGGER.warning(
            'Skipping malformed query result: invalid Wikidata property URI %s in %s', pid_uri, result)
        return None
    return pid


def identifier_class_based_query(ontology_class, identifier_property, results_per_page):
    query = IDENTIFIER_CLASS_BASED_QUERY_TEMPLATE % (
        ontology_class, identifier_property)
    return _parse_query_result('identifier', _run_paged_query(results_per_page, query))


def _parse_query_result(query_type, result_set):
    # Paranoid checks for malformed results:
    # it should never happen, but it actually does
    for result in result_set:
        if query_type == 'identifier':
            identifier_or_link = result.get(IDENTIFIER_BINDING)
            to_be_logged = 'external identifier'
        elif query_type == 'links':
            identifier_or_link = result.get(LINK_BINDING)
            to_be_logged = 'third-party URL'
        if not identifier_or_link:
            LOGGER.warning(
                'Skipping malformed query result: no %s in %s', to_be_logged, result)
            continue
        valid_qid = _get_valid_qid(result)
        if not valid_qid:
            continue
        yield {valid_qid.group(): identifier_or_link}


def _get_valid_qid(result):
    item_uri = result.get(ITEM_BINDING)
    if not item_uri:
        LOGGER.warning(
            'Skipping malformed query result: no Wikidata item in %s', result)
        return None
    qid = search(ITEM_REGEX, item_uri)
    if not qid:
        LOGGER.warning(
            'Skipping malformed query result: invalid Wikidata item URI %s in %s', item_uri, result)
        return None
    return qid


def _run_paged_query(result_per_page, query):
    if result_per_page == 0:
        LOGGER.info('Running query without paging: %s', query)
        result_set = make_request(query)
        if not result_set:
            LOGGER.error('The query went wrong')
            yield {}
        if result_set == 'empty':
            LOGGER.warning('Empty result')
            yield {}
        for result in result_set:
            yield result
    else:
        LOGGER.info('Running paged query with %d results per page: %s',
                    result_per_page, query)
        pages = 0
        while True:
            LOGGER.info('Page #%d', pages)
            query_builder = [query]
            query_builder.append('OFFSET %d LIMIT %d' %
                                 (result_per_page * pages, result_per_page))
            result_set = make_request(' '.join(query_builder))
            if not result_set:
                LOGGER.error(
                    'Skipping page %d because the query went wrong', pages)
                pages += 1
                continue
            if result_set == 'empty':
                LOGGER.info('Paging finished. Total pages: %d', pages)
                break
            for result in result_set:
                yield result
            pages += 1


@click.command()
@click.argument('identifier_property')
@click.argument('occupation_class')
@click.option('-p', '--results-per-page', default=1000, help='default: 1000')
@click.option('-o', '--outdir', type=click.Path(), default='output', help="default: 'output'")
def identifier_occupation_based_query_cli(identifier_property, occupation_class, results_per_page, outdir):
    """Run a paged SPARQL query against the Wikidata endpoint to get items and external catalog
    identifiers. Dump the result into a JSONlines file.

    IDENTIFIER_PROPERTY must be a Wikidata property identifier like 'P1953' (Discogs artist ID).

    OCCUPATION_CLASS must be a Wikidata ontology class like 'Q639669' (musician).

    Use '-p 0' to switch paging off.
    """
    with open(os.path.join(outdir, 'occupation_based_identifier_query_result.jsonl'), 'w', 1) as outfile:
        _dump_result(identifier_occupation_based_query(
            occupation_class, identifier_property, results_per_page), outfile)
        LOGGER.info(
            "Occupation-based identifier query result dumped as JSON lines to '%s'", outfile.name)


def identifier_occupation_based_query(occupation_class, identifier_property, results_per_page):
    query = IDENTIFIER_OCCUPATION_BASED_QUERY_TEMPLATE % (
        occupation_class, identifier_property)
    return _parse_query_result('identifier', _run_paged_query(results_per_page, query))


@click.command()
@click.argument('items_file', type=click.File())
@click.argument('condition_pattern')
@click.option('-b', '--bucket-size', default=500, help='Default: 500.')
@click.option('-o', '--outdir', type=click.Path(), default='output',
              help="Default: 'output'.")
def values_query(items_file, condition_pattern, bucket_size, outdir):
    """Run a SPARQL query against the Wikidata endpoint using buckets of item values
    and dump the result into a JSONlines file.

    CONSTRAINT must be a property + value pattern like 'wdt:P434 ?musicbrainz'.
    """
    entities = ['wd:%s' % l.rstrip() for l in items_file.readlines()]
    buckets = [entities[i*bucket_size:(i+1)*bucket_size]
               for i in range(0, int((len(entities)/bucket_size+1)))]
    with open(os.path.join(outdir, 'values_query_result.jsonl'), 'w', 1) as outfile:
        for bucket in buckets:
            query = VALUES_QUERY_TEMPLATE % (
                ' '.join(bucket), condition_pattern)
            result_set = make_request(query)
            if not result_set:
                LOGGER.warning('Skipping bucket that went wrong')
                continue
            if result_set == 'empty':
                LOGGER.warning('Skipping bucket with no results')
                continue
            _dump_result(result_set, outfile)
        LOGGER.info(
            "Values query result dumped as JSON lines to '%s'", outfile.name)


def _dump_result(result_set, outfile):
    for row in result_set:
        outfile.write(json.dumps(row, ensure_ascii=False) + '\n')


def make_request(query, response_format=DEFAULT_RESPONSE_FORMAT):
    response = get(WIKIDATA_SPARQL_ENDPOINT, params={
        'query': query}, headers={'Accept': response_format})
    log_request_data(response, LOGGER)
    if response.ok:
        LOGGER.debug(
            'Successful GET to the Wikidata SPARQL endpoint. Status code: %d', response.status_code)
        if response_format == JSON_RESPONSE_FORMAT:
            LOGGER.debug('Returning JSON results')
            return response.json()
        response_body = response.text.splitlines()
        if len(response_body) == 1:
            LOGGER.debug('Got an empty result set from query: %s', query)
            return 'empty'
        LOGGER.debug('Got %d results', len(response_body) - 1)
        return DictReader(response_body, delimiter='\t')
    LOGGER.warning(
        'The GET to the Wikidata SPARQL endpoint went wrong. Reason: %d %s - Query: %s',
        response.status_code, response.reason, query)
    return None


def query_info_for(qids_bucket, properties):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    query = """SELECT * WHERE{ VALUES ?id { %s } """ % ' '.join(qids_bucket)
    for i in properties:
        query += """OPTIONAL { ?id wdt:%s ?%s . } """ % (i, i)
    query += """}"""
    return query


def query_wikipedia_articles_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting wikidata articles"""

    query = """SELECT * WHERE{ VALUES ?id { %s } """ % ' '.join(qids_bucket)
    query += """OPTIONAL { ?article schema:about ?id . }"""
    query += """}"""
    return query


def query_birth_death(qids_bucket):
    """Given a list of wikidata entities returns a query for getting their birth and death dates"""

    query = """SELECT ?id ?birth ?b_precision ?death ?d_precision WHERE{ VALUES ?id { %s } """ % ' '.join(
        qids_bucket)
    query += """?id p:P569 ?b. ?b psv:P569 ?t1 . ?t1 wikibase:timePrecision ?b_precision . ?t1 wikibase:timeValue ?birth . OPTIONAL { ?id p:P570 ?d . ?d psv:P570 ?t2 . ?t2 wikibase:timePrecision ?d_precision . ?t2 wikibase:timeValue ?death . }"""
    query += """}"""

    return query
