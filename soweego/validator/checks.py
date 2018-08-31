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
from csv import DictReader

import click
from soweego.wikidata.sparql_queries import (instance_based_identifier_query,
                                             occupation_based_identifier_query)

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('wikidata_query_type', type=click.Choice(['instance', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('target_identifiers', type=click.File())
@click.option('-o', '--outdir', type=click.Path(), default='output', help="default: 'output'")
def check_existence(wikidata_query_type, class_qid, catalog_pid, target_identifiers, outdir):
    """Check the existence of identifier statements.

    Dump a JSON file of nonexistent ones ``{identifier: QID}``
    """
    # TODO for each wikidata_items item, do a binary search on the target list
    if wikidata_query_type == 'instance':
        query_function = instance_based_identifier_query
    elif wikidata_query_type == 'occupation':
        query_function = occupation_based_identifier_query

    qids_to_ids = {}
    invalid = defaultdict(list)
    for row in query_function(class_qid, catalog_pid, 1000):
        qids_to_ids.update(row)

    nonexistent_identifiers = set(qids_to_ids.keys()).difference(
        set(i.rstrip() for i in target_identifiers))
    for identifier in nonexistent_identifiers:
        invalid[identifier].append(qids_to_ids[identifier])
    json.dump(invalid, outdir, indent=2)
