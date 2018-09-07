#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A Wikidata bot that adds referenced identifier statements as in the following example.

Claim = (Joey Ramone, MusicBrainz artist ID, 2f3f8fb1-e5dc-4548-9601-fada0485e561)
Reference = [ (stated in, MusicBrainz), (retrieved, TIMESTAMP) ]
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
from datetime import date

import click
import pywikibot

from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

SITE = pywikibot.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()

# (stated in, CATALOG) reference object
STATED_IN_REFERENCE = pywikibot.Claim(
    REPO, vocabulary.STATED_IN_PID, is_reference=True)

# (retrieved, TIMESTAMP) reference object
TODAY = date.today()
TIMESTAMP = pywikibot.WbTime(
    site=REPO, year=TODAY.year, month=TODAY.month, day=TODAY.day, precision='day')
RETRIEVED_REFERENCE = pywikibot.Claim(
    REPO, vocabulary.RETRIEVED_PID, is_reference=True)
RETRIEVED_REFERENCE.setTarget(TIMESTAMP)


@click.command()
@click.argument('mapping', type=click.File())
@click.argument('catalog_name', type=click.Choice(['bibsys', 'discogs', 'musicbrainz', 'twitter']))
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in the Wikidata sandbox item')
def add_identifiers_cli(mapping, catalog_name, sandbox):
    """Bot add identifiers to existing Wikidata items.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    add_identifiers(json.load(mapping), catalog_name, sandbox)


def add_identifiers(mapping: dict, catalog_name: str, sandbox: bool):
    """Add identifier statements to existing Wikidata items.

    :param mapping: a ``{QID: catalog_identifier}`` dictionary
    :type mapping: dict
    :param catalog_name: the name of the target catalog, e.g., ``musicbrainz``
    :type catalog_name: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item
    :type sandbox: bool
    """
    for qid, catalog_id in mapping.items():
        LOGGER.info('Processing %s match: %s -> %s',
                    catalog_name, qid, catalog_id)
        if not sandbox:
            _add_identifier(qid, catalog_id, catalog_name)
        else:
            _add_identifier(vocabulary.SANDBOX_1_QID, catalog_id, catalog_name)


def _add_identifier(qid: str, catalog_id: str, catalog_name: str):
    subject = pywikibot.ItemPage(REPO, qid)
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog_name)
    claim = pywikibot.Claim(REPO, catalog_terms['pid'])
    claim.setTarget(catalog_id)
    subject.addClaim(claim)
    LOGGER.debug('Claim added: %s', claim.toJSON())
    STATED_IN_REFERENCE.setTarget(
        pywikibot.ItemPage(REPO, catalog_terms['qid']))
    claim.addSources([STATED_IN_REFERENCE, RETRIEVED_REFERENCE])
    LOGGER.debug('Reference node added: %s, %s',
                 STATED_IN_REFERENCE.toJSON(), RETRIEVED_REFERENCE.toJSON())
    LOGGER.info('%s identifier statement added to %s', catalog_name, qid)
