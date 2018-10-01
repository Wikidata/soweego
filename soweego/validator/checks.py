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

import click

from soweego.wikidata.sparql_queries import (instance_based_identifier_query,
                                             occupation_based_identifier_query)

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('wikidata_query_type', type=click.Choice(['instance', 'occupation']))
@click.argument('class_qid')
@click.argument('catalog_pid')
@click.argument('target_identifiers', type=click.File())
@click.option('-o', '--outfile', type=click.File('w'), default='output/non_existent_ids.json', help="default: 'output/non_existent_ids.json'")
def check_existence_cli(wikidata_query_type, class_qid, catalog_pid, target_identifiers, outfile):
    """Check the existence of identifier statements.

    Dump a JSON file of invalid ones ``{identifier: QID}``
    """
    invalid = check_existence(wikidata_query_type, class_qid,
                              catalog_pid, target_identifiers)
    json.dump(invalid, outfile, indent=2)


def check_existence(wikidata_query_type, class_qid, catalog_pid, target_identifiers):
    # TODO for each wikidata_items item, do a binary search on the target list
    if wikidata_query_type == 'instance':
        query_function = instance_based_identifier_query
    elif wikidata_query_type == 'occupation':
        query_function = occupation_based_identifier_query

    target_ids_set = set(target_id.rstrip()
                         for target_id in target_identifiers)
    invalid = defaultdict(set)
    count = 0
    for row in query_function(class_qid, catalog_pid, 1000):
        for qid, target_id in row.items():
            if target_id not in target_ids_set:
                LOGGER.warning(
                    '%s identifier %s is invalid', qid, target_id)
                invalid[target_id].add(qid)
                count += 1
    LOGGER.info('Total invalid identifiers = %d', count)
    # Sets are not serializable to JSON, so cast them to lists
    return {target_id: list(qids) for target_id, qids in invalid.items()}
