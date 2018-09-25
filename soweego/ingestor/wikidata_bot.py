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
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('matches', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in the Wikidata sandbox item')
def add_identifiers_cli(catalog_name, matches, sandbox):
    """Bot add identifiers to existing Wikidata items.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    add_identifiers(json.load(matches), catalog_name, sandbox)


@click.command()
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('invalid_identifiers', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in a random Wikidata sandbox item')
def delete_identifiers_cli(catalog_name, invalid_identifiers, sandbox):
    """Bot delete invalid identifiers from existing Wikidata items.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    delete_identifiers(json.load(invalid_identifiers), catalog_name, sandbox)


def add_identifiers(matches: dict, catalog_name: str, sandbox: bool) -> None:
    """Add identifier statements to existing Wikidata items.

    :param matches: a ``{QID: catalog_identifier}`` dictionary
    :type matches: dict
    :param catalog_name: the name of the target catalog, e.g., ``musicbrainz``
    :type catalog_name: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item
    :type sandbox: bool
    """
    for qid, catalog_id in matches.items():
        LOGGER.info('Processing %s match: %s -> %s',
                    catalog_name, qid, catalog_id)
        if sandbox:
            _add_or_delete('add', vocabulary.SANDBOX_1_QID,
                           catalog_id, catalog_name)
        else:
            _add_or_delete('add', qid, catalog_id, catalog_name)


def _add_or_delete(action, qid, catalog_id, catalog_name):
    actions = {
        'add': _add_identifier,
        'delete': _delete_identifier
    }
    actions[action](qid, catalog_id, catalog_name)


def delete_identifiers(invalid: dict, catalog_name: str, sandbox: bool) -> None:
    """Delete invalid identifier statements from existing Wikidata items.

    Identifiers that should be deleted come from the first validation check
    as per :func:`soweego.validator.checks.check_existence`.

    :param invalid: a ``{invalid_catalog_identifier: [list of QIDs]}`` dictionary
    :type invalid: dict
    :param catalog_name: the name of the target catalog, e.g., ``discogs``
    :type catalog_name: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item
    :type sandbox: bool
    """
    for catalog_id, qids in invalid.items():
        for qid in qids:
            LOGGER.info('Deleting %s identifier: %s -> %s',
                        catalog_name, qid, catalog_id)
            if sandbox:
                _add_or_delete('delete', vocabulary.SANDBOX_1_QID,
                               catalog_id, catalog_name)
            else:
                _add_or_delete('delete', qid, catalog_id, catalog_name)


def _add_identifier(qid: str, catalog_id: str, catalog_name: str) -> None:
    subject = pywikibot.ItemPage(REPO, qid)
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog_name)
    claim = pywikibot.Claim(REPO, catalog_terms['pid'])
    claim.setTarget(catalog_id)
    subject.addClaim(claim)
    LOGGER.debug('Added claim: %s', claim.toJSON())
    STATED_IN_REFERENCE.setTarget(
        pywikibot.ItemPage(REPO, catalog_terms['qid']))
    claim.addSources([STATED_IN_REFERENCE, RETRIEVED_REFERENCE])
    LOGGER.debug('Added reference node: %s, %s',
                 STATED_IN_REFERENCE.toJSON(), RETRIEVED_REFERENCE.toJSON())
    LOGGER.info('Added %s identifier statement to %s', catalog_name, qid)


def _delete_identifier(qid: str, catalog_id: str, catalog_name: str) -> None:
    item = pywikibot.ItemPage(REPO, qid)
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog_name)
    item_data = item.get()
    item_claims = item_data.get('claims')
    # This should not happen:
    # the input item is supposed to have at least an identifier claim.
    # We never know, Wikidata is live.
    if not item_claims:
        LOGGER.error('%s has no claims. Cannot delete %s identifier %s',
                     qid, catalog_name, catalog_id)
        return
    catalog_pid = catalog_terms['pid']
    identifier_claims = item_claims.get(catalog_pid)
    # Same comment as the previous one
    if not identifier_claims:
        LOGGER.error('%s has no %s claims. Cannot delete %s identifier %s',
                     qid, catalog_pid, catalog_name, catalog_id)
        return
    for claim in identifier_claims:
        if claim.getTarget() == catalog_id:
            item.removeClaims([claim], summary='Invalid identifier')
            LOGGER.debug('Deleted claim: %s', claim.toJSON())
    LOGGER.info('Deleted %s identifier statement from %s', catalog_name, qid)
