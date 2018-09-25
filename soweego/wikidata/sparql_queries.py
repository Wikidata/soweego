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
import os
from csv import DictReader
from re import search

import click
from requests import get

from soweego.commons.logging import log_request_data

LOGGER = logging.getLogger(__name__)

ITEM_REGEX = r'Q\d+'

WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'

ITEM_BINDING = '?item'
IDENTIFIER_BINDING = '?identifier'

CLASS_BASED_IDENTIFIER_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + IDENTIFIER_BINDING + \
    ' WHERE { ' + ITEM_BINDING + \
    ' wdt:P31/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + ' . }'
OCCUPATION_BASED_IDENTIFIER_QUERY_TEMPLATE = 'SELECT DISTINCT ' + ITEM_BINDING + ' ' + IDENTIFIER_BINDING + \
    ' WHERE { ' + ITEM_BINDING + \
    ' wdt:P106/wdt:P279* wd:%s ; wdt:%s ' + IDENTIFIER_BINDING + ' . }'
VALUES_QUERY_TEMPLATE = 'SELECT * WHERE { VALUES ' + \
    ITEM_BINDING + ' { %s } . ' + ITEM_BINDING + ' %s }'


@click.command()
@click.argument('ontology_class')
@click.argument('identifier_property')
@click.option('-p', '--results-per-page', default=1000, help='default: 1000')
@click.option('-o', '--outdir', type=click.Path(), default='output', help="default: 'output'")
def instance_based_identifier_query_cli(ontology_class, identifier_property, results_per_page, outdir):
    """Run a paged SPARQL query against the Wikidata endpoint to get items and external catalog
    identifiers. Dump the result into a JSONlines file.

    IDENTIFIER_PROPERTY must be a Wikidata property identifier like 'P1953' (Discogs artist ID).

    ONTOLOGY_CLASS must be a Wikidata ontology class like 'Q5' (human).

    Use '-p 0' to switch paging off.
    """
    with open(os.path.join(outdir, 'class_based_identifier_query_result.jsonl'), 'w', 1) as outfile:
        _dump_result(instance_based_identifier_query(
            ontology_class, identifier_property, results_per_page), outfile)
        LOGGER.info(
            "Class-based identifier query result dumped as JSON lines to '%s'", outfile.name)


def instance_based_identifier_query(ontology_class, identifier_property, results_per_page):
    query = CLASS_BASED_IDENTIFIER_QUERY_TEMPLATE % (
        ontology_class, identifier_property)
    return _parse_identifier_query_result(_run_identifier_query(results_per_page, query))


def _parse_identifier_query_result(result_set):
    # Paranoid checks for malformed results:
    # it should never happen, but sometimes it does
    for result in result_set:
        item_uri = result.get(ITEM_BINDING)
        if not item_uri:
            LOGGER.error(
                'Skipping malformed query result: no Wikidata item in %s', result)
            continue
        identifier = result.get(IDENTIFIER_BINDING)
        if not identifier:
            LOGGER.error(
                'Skipping malformed query result: no external identifier in %s', result)
            continue
        qid = search(ITEM_REGEX, item_uri)
        if not qid:
            LOGGER.error(
                'Skipping malformed query result: invalid Wikidata item URI %s in %s', item_uri, result)
            continue
        yield {qid.group(): identifier}


def _run_identifier_query(result_per_page, query):
    if result_per_page == 0:
        LOGGER.info('Running query without paging: %s', query)
        for result in _make_request(query):
            yield result
    else:
        LOGGER.info('Running paged query: %s', query)
        pages = 0
        while True:
            LOGGER.info('Page #%d', pages)
            query_builder = [query]
            query_builder.append('OFFSET %d LIMIT %d' %
                                 (result_per_page * pages, result_per_page))
            result_set = _make_request(' '.join(query_builder))
            if not result_set:
                LOGGER.error('Stopping paging because the query went wrong')
                break
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
def occupation_based_identifier_query_cli(identifier_property, occupation_class, results_per_page, outdir):
    """Run a paged SPARQL query against the Wikidata endpoint to get items and external catalog
    identifiers. Dump the result into a JSONlines file.

    IDENTIFIER_PROPERTY must be a Wikidata property identifier like 'P1953' (Discogs artist ID).

    OCCUPATION_CLASS must be a Wikidata ontology class like 'Q639669' (musician).

    Use '-p 0' to switch paging off.
    """
    with open(os.path.join(outdir, 'occupation_based_identifier_query_result.jsonl'), 'w', 1) as outfile:
        _dump_result(occupation_based_identifier_query(
            occupation_class, identifier_property, results_per_page), outfile)
        LOGGER.info(
            "Occupation-based identifier query result dumped as JSON lines to '%s'", outfile.name)


def occupation_based_identifier_query(occupation_class, identifier_property, results_per_page):
    query = OCCUPATION_BASED_IDENTIFIER_QUERY_TEMPLATE % (
        occupation_class, identifier_property)
    return _parse_identifier_query_result(_run_identifier_query(results_per_page, query))


@click.command()
@click.argument('items_file', type=click.File())
@click.argument('condition_pattern')
@click.option('-b', '--bucket-size', default=500, help="default: 500")
@click.option('-o', '--outdir', type=click.Path(), default='output',
              help="default: 'output'")
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
            result_set = _make_request(query)
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


def _make_request(query):
    request = get(WIKIDATA_SPARQL_ENDPOINT, params={
        'query': query}, headers={'Accept': 'text/tab-separated-values'})
    log_request_data(request, LOGGER)
    if request.ok:
        LOGGER.debug(
            'Successful GET to the Wikidata SPARQL endpoint. Status code: %d', request.status_code)
        response_body = request.text.splitlines()
        if len(response_body) == 1:
            LOGGER.debug('Got an empty result set from query: %s', query)
            return 'empty'
        LOGGER.debug('Got %d results', len(response_body))
        return DictReader(response_body, delimiter='\t')
    else:
        LOGGER.warning(
            'The GET to the Wikidata SPARQL endpoint went wrong. Reason: %d %s - Query: %s',
            request.status_code, request.reason, query)
        return None
