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

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('wikidata_items', type=click.File())
@click.argument('target_identifiers', type=click.File())
@click.option('-o', '--outfile', type=click.File('w'), default='output/nonexistent_identifiers.json')
def check_existence(wikidata_items, target_identifiers, outfile):
    """Check the existence of identifier statements.

    Dump a JSON file of nonexistent ones ``{identifier: QID}``
    """
    # TODO for each wikidata_items item, do a binary search on the target list
    qids_to_ids = {}
    invalid = defaultdict(list)
    csv_wikidata_items = DictReader(wikidata_items, delimiter='\t')
    fields = csv_wikidata_items.fieldnames
    for row in csv_wikidata_items:
        qids_to_ids[row[fields[1]]] = row[fields[0]]
    nonexistent_identifiers = set(
        qids_to_ids.keys()).difference(set(i.rstrip() for i in target_identifiers))
    for identifier in nonexistent_identifiers:
        invalid[identifier].append(qids_to_ids[identifier])
    json.dump(invalid, outfile, indent=2)
