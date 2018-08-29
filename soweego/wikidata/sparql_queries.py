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

import click
from requests import get

from soweego.commons.logging import log_request_data

LOGGER = logging.getLogger(__name__)
WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'
VALUES_QUERY_TEMPLATE = 'SELECT * WHERE { VALUES ?item { %s } . %s }'


@click.command()
@click.argument('sparql_query')
@click.option('-p', '--paging', default=1000, help='default: 1000')
@click.option('-o', '--outfile', type=click.Path(), default='output/generic_query_result.jsonl')
def generic_query(sparql_query, paging, outfile):
    """Run a given SPARQL query against the Wikidata endpoint with a paging mechanism
    and dump the results into a JSONlines file.

    Use '-p 0' to switch paging off.
    """
    if paging == 0:
        _make_request(sparql_query, outfile)
    else:
        query_builder = [sparql_query]
        query_builder.append(('OFFSET %d LIMIT %d' % ()))


@click.command()
@click.argument('items_file', type=click.File())
@click.argument('condition_pattern')
@click.option('-b', '--bucket-size', default=500, help="default: 500")
@click.option('-o', '--outdir', type=click.Path(file_okay=False), default='output',
              help="default: 'output'")
def values_query(items_file, condition_pattern, bucket_size, outdir):
    """Run a SPARQL query against the Wikidata endpoint using batches of items.

    CONDITION_PATTERN must be a triple pattern with ?item as a binding for subject items.
    """
    entities = ['wd:%s' % l.rstrip() for l in items_file.readlines()]
    buckets = [entities[i*bucket_size:(i+1)*bucket_size]
               for i in range(0, int((len(entities)/bucket_size+1)))]
    with open(os.path.join(outdir, 'values_query_result.jsonl'), 'w', 1) as outfile:
        for bucket in buckets:
            query = VALUES_QUERY_TEMPLATE % (
                ' '.join(bucket), condition_pattern)
            result_set = _make_request(query)
            for row in result_set:
                outfile.write(json.dumps(row, ensure_ascii=False) + '\n')
        LOGGER.info(
            "SPARQL query results dumped as JSON lines to '%s'", outfile.name)


def _make_request(query):
    request = get(WIKIDATA_SPARQL_ENDPOINT, params={
        'query': query}, headers={'Accept': 'text/tab-separated-values'})
    log_request_data(request, LOGGER)
    if request.ok:
        LOGGER.debug(
            'Successful GET to the Wikidata SPARQL endpoint. Status code: %d', request.status_code)
        return DictReader(request.text.splitlines(), delimiter='\t')
    else:
        LOGGER.warning(
            'Will not dump results: the GET to the Wikidata SPARQL endpoint went wrong. Reason: %d %s - Query: %s',
            request.status_code, request.reason, query)
        return None
