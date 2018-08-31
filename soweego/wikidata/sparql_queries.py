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

LOGGER = logging.getLogger(__name__)
WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'


@click.command()
@click.argument('sparql_query')
@click.option('-p', '--paging', default=1000, help='default: 1000')
@click.option('-o', '--outfile', type=click.Path(), default='output/query_results.jsonl')
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
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='output',
              help="default: 'output'")
def values_query(items_file, condition_pattern, bucket_size, output_dir):
    """Run a SPARQL query against the Wikidata endpoint using batches of items.

    CONDITION_PATTERN must be a triple pattern with ?item as a binding for subject items.
    """
    entities = ['wd:%s' % l.rstrip() for l in items_file.readlines()]
    buckets = [entities[i*bucket_size:(i+1)*bucket_size]
               for i in range(0, int((len(entities)/bucket_size+1)))]
    with open(os.path.join(output_dir, 'results.tsv'), 'w', 1) as o:
        for b in buckets:
            query = 'select * where { values ?item { %s } . %s }' % (
                ' '.join(b), condition_pattern)
            o.write(json.dumps(_make_request(query)))
    return 0


def _make_request(query):
    request = get(WIKIDATA_SPARQL_ENDPOINT, params={
        'query': query}, headers={'Accept': 'text/tab-separated-values'})
    if request.ok:
        LOGGER.debug(
            'Correct HTTP request to the Wikidata SPARQL endpoint. Status code: %d - Query: %s', request.status_code, query)
        return DictReader(request.text.splitlines(), delimiter='\t')
    else:
        LOGGER.warning(
            'Will not dump results: the HTTP request to the Wikidata SPARQL endpoint went wrong. Reason: %d %s - Query: %s',
            request.status_code, request.reason, query)
        return


def qid_id_by_pid(pid):
    """Given a Property ID, generates {qid: targetID}"""
    page_number = 0
    limit = 0
    while limit == 0:
        query = """SELECT ?human ?id
                WHERE { ?human wdt:P31 wd:Q5 . ?human wdt:%s ?id . } 
                ORDER BY(?human) LIMIT 1000 OFFSET %s""" % (pid, page_number * 1000)
        limit = 1000
        result = _make_request(query)

        for row in result:
            qid = row['?human'][1:-1].split('/')[-1]
            limit -= 1
            yield {qid: row['?id']}

        page_number += 1
