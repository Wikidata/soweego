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
    REPO, vocabulary.STATED_IN, is_reference=True)

# (retrieved, TIMESTAMP) reference object
TODAY = date.today()
TIMESTAMP = pywikibot.WbTime(
    site=REPO, year=TODAY.year, month=TODAY.month, day=TODAY.day, precision='day')
RETRIEVED_REFERENCE = pywikibot.Claim(
    REPO, vocabulary.RETRIEVED, is_reference=True)
RETRIEVED_REFERENCE.setTarget(TIMESTAMP)


@click.command()
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('matches', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in the Wikidata sandbox item Q4115189.')
def add_identifiers_cli(catalog_name, matches, sandbox):
    """Add identifiers to existing Wikidata items.

    MATCHES must be a { QID: catalog_identifier } JSON file.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    add_identifiers(json.load(matches), catalog_name, sandbox)


@click.command()
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('statements', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in the Wikidata sandbox item Q4115189.')
def add_statements_cli(catalog_name, statements, sandbox):
    """Add statements to existing Wikidata items.

    STATEMENTS must be a subject, predicate, value TSV file.
    """
    stated_in = vocabulary.CATALOG_MAPPING.get(catalog_name)['qid']
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    for statement in statements:
        subject, predicate, value = statement.rstrip().split('\t')
        if sandbox:
            _add_or_reference(vocabulary.SANDBOX_1,
                              predicate, value, stated_in)
        else:
            _add_or_reference(subject, predicate, value, stated_in)


@click.command()
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('invalid_identifiers', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits in a random Wikidata sandbox item.')
def delete_identifiers_cli(catalog_name, invalid_identifiers, sandbox):
    """Delete invalid identifiers from existing Wikidata items.

    INVALID_IDENTIFIERS must be a { QID: catalog_identifier } JSON file.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    delete_or_deprecate_identifiers('delete', json.load(
        invalid_identifiers), catalog_name, sandbox)


@click.command()
@click.argument('catalog_name', type=click.Choice(['discogs', 'imdb', 'musicbrainz', 'twitter']))
@click.argument('invalid_identifiers', type=click.File())
@click.option('-s', '--sandbox', is_flag=True, help='Perform all edits on the Wikidata sandbox item Q4115189')
def deprecate_identifiers_cli(catalog_name, invalid_identifiers, sandbox):
    """Deprecate invalid identifiers from existing Wikidata items.

    INVALID_IDENTIFIERS must be a { QID: catalog_identifier } JSON file.
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item')
    delete_or_deprecate_identifiers('deprecate', json.load(
        invalid_identifiers), catalog_name, sandbox)


def add_identifiers(matches: dict, catalog_name: str, sandbox: bool) -> None:
    """Add identifier statements to existing Wikidata items.

    :param matches: a ``{QID: catalog_identifier}`` dictionary
    :type matches: dict
    :param catalog_name: the name of the target catalog, e.g., ``musicbrainz``
    :type catalog_name: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item Q4115189
    :type sandbox: bool
    """
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog_name)
    for qid, catalog_id in matches.items():
        LOGGER.info('Processing %s match: %s -> %s',
                    catalog_name, qid, catalog_id)
        if sandbox:
            LOGGER.info(
                'Using Wikidata sandbox item %s as subject, instead of %s', vocabulary.SANDBOX_1, qid)
            _add_or_reference(vocabulary.SANDBOX_1,
                              catalog_terms['pid'], catalog_id, catalog_terms['qid'])
        else:
            _add_or_reference(
                qid, catalog_terms['pid'], catalog_id, catalog_terms['qid'])


def add_statements(statements: list, stated_in_catalog: str, sandbox: bool) -> None:
    """Add generic statements to existing Wikidata items.

    Addition candidates typically come from validation criteria 2 or 3
    as per :func:`soweego.validator.checks.check_links` and
    :func:`soweego.validator.checks.check_metadata`.

    :param statements: list of (subject, predicate, value) triples
    :type statements: list
    :param stated_in_catalog: QID of the target catalog where statements come from
    :type stated_in_catalog: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item Q4115189
    :type sandbox: bool
    """
    for subject, predicate, value in statements:
        LOGGER.info('Processing (%s, %s, %s) statement')
        if sandbox:
            _add_or_reference(vocabulary.SANDBOX_1,
                              predicate, value, stated_in_catalog)
        else:
            _add_or_reference(subject, predicate, value, stated_in_catalog)


def delete_or_deprecate_identifiers(action: str, invalid: dict, catalog_name: str, sandbox: bool) -> None:
    """Delete or deprecate invalid identifier statements from existing Wikidata items.

    Deletion candidates come from the validation criterion 1
    as per :func:`soweego.validator.checks.check_existence`.

    Deprecation candidates come from validation criteria 2 or 3
    as per :func:`soweego.validator.checks.check_links` and
    :func:`soweego.validator.checks.check_metadata`.

    :param action: either ``delete`` or ``deprecate``
    :type action: str
    :param invalid: a ``{invalid_catalog_identifier: [list of QIDs]}`` dictionary
    :type invalid: dict
    :param catalog_name: the name of the target catalog, e.g., ``discogs``
    :type catalog_name: str
    :param sandbox: whether to perform edits on the Wikidata sandbox item Q4115189
    :type sandbox: bool
    """
    for catalog_id, qids in invalid.items():
        for qid in qids:
            LOGGER.info('Will %s %s identifier: %s -> %s',
                        action, catalog_name, qid, catalog_id)
            if sandbox:
                _delete_or_deprecate(action, vocabulary.SANDBOX_1,
                                     catalog_id, catalog_name)
            else:
                _delete_or_deprecate(action, qid, catalog_id, catalog_name)


def _add_or_reference(subject: str, predicate: str, value: str, stated_in: str) -> None:
    item = pywikibot.ItemPage(REPO, subject)

    # get redirect target recursively in case a redirect points
    # to another redirect
    while item.isRedirectPage():
        item = item.getRedirectTarget()

    data = item.get()
    # No data at all
    if not data:
        LOGGER.warning('%s has no data at all', subject)
        _add(item, predicate, value, stated_in)
        return
    claims = data.get('claims')
    # No claims
    if not claims:
        LOGGER.warning('%s has no claims', subject)
        _add(item, predicate, value, stated_in)
        return
    # Check 1: same value in 'official website' property -> add reference
    # See https://www.wikidata.org/wiki/User_talk:Jura1#Thanks_for_your_feedback_on_User:Soweego_bot_task_2
    official_websites = claims.get(vocabulary.OFFICIAL_WEBSITE)
    if official_websites:
        for claim in official_websites:
            if claim.getTarget() == value:
                LOGGER.debug(
                    "%s has an official website claim with value '%s'", subject, value)
                _reference(claim, stated_in)
                return
    given_predicate_claims = claims.get(predicate)
    # Check 2: no claim with the given predicate -> add statement
    if not given_predicate_claims:
        LOGGER.debug('%s has no %s claim', subject, predicate)
        _add(item, predicate, value, stated_in)
        return
    # Check 3: handle case-insensitive IDs: Facebook, Twitter
    # See https://www.wikidata.org/wiki/Topic:Unym71ais48bt6ih
    case_insensitive = True if predicate in [
        vocabulary.FACEBOOK_PID, vocabulary.TWITTER_USERNAME_PID] else False
    if case_insensitive:
        value = value.lower()
        existing_values = [claim_value.getTarget().lower()
                           for claim_value in given_predicate_claims]
    else:
        existing_values = [claim_value.getTarget()
                           for claim_value in given_predicate_claims]
    # No given value -> add statement
    if value not in existing_values:
        LOGGER.debug('%s has no %s claim with value %s',
                     subject, predicate, value)
        _add(item, predicate, value, stated_in)
        return
    # Claim with the given predicate and value -> add reference
    LOGGER.debug("%s has a %s claim with value '%s'",
                 subject, predicate, value)
    if case_insensitive:
        for claim in given_predicate_claims:
            if claim.getTarget().lower() == value:
                _reference(claim, stated_in)
                return
    for claim in given_predicate_claims:
        if claim.getTarget() == value:
            _reference(claim, stated_in)


def _add(subject_item, predicate, value, stated_in):
    claim = pywikibot.Claim(REPO, predicate)
    claim.setTarget(value)
    subject_item.addClaim(claim)
    LOGGER.debug('Added claim: %s', claim.toJSON())
    _reference(claim, stated_in)
    LOGGER.info('Added (%s, %s, %s) statement',
                subject_item.getID(), predicate, value)


def _reference(claim, stated_in):
    STATED_IN_REFERENCE.setTarget(
        pywikibot.ItemPage(REPO, stated_in))
    claim.addSources([STATED_IN_REFERENCE, RETRIEVED_REFERENCE])
    LOGGER.info('Added (%s, %s), (%s, %s) reference node', STATED_IN_REFERENCE.getID(
    ), stated_in, RETRIEVED_REFERENCE.getID(), TODAY)


def _delete_or_deprecate(action: str, qid: str, catalog_id: str, catalog_name: str) -> None:
    item = pywikibot.ItemPage(REPO, qid)
    catalog_terms = vocabulary.CATALOG_MAPPING.get(catalog_name)
    item_data = item.get()
    item_claims = item_data.get('claims')
    # This should not happen:
    # the input item is supposed to have at least an identifier claim.
    # We never know, Wikidata is alive.
    if not item_claims:
        LOGGER.error('%s has no claims. Cannot %s %s identifier %s',
                     qid, action, catalog_name, catalog_id)
        return
    catalog_pid = catalog_terms['pid']
    identifier_claims = item_claims.get(catalog_pid)
    # Same comment as the previous one
    if not identifier_claims:
        LOGGER.error('%s has no %s claims. Cannot %s %s identifier %s',
                     qid, catalog_pid, action, catalog_name, catalog_id)
        return
    for claim in identifier_claims:
        if claim.getTarget() == catalog_id:
            if action == 'delete':
                item.removeClaims([claim], summary='Invalid identifier')
            elif action == 'deprecate':
                claim.changeRank(
                    'deprecated', summary='Deprecate arguable claim')
            LOGGER.debug('%s claim: %s', action.title() + 'd', claim.toJSON())
    LOGGER.info('%s %s identifier statement from %s',
                action.title() + 'd', catalog_name, qid)
