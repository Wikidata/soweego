#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A `Wikidata bot <https://www.wikidata.org/wiki/Wikidata:Bots>`_ that adds, deletes, or deprecates referenced statements.
Here are typical output examples:

:func:`add_identifiers`
  | *Claim:* `Joey Ramone <https://www.wikidata.org/wiki/Q312387>`_, `Discogs artist ID <https://www.wikidata.org/wiki/Property:P1953>`_, `264375 <https://www.discogs.com/artist/264375>`_
  | *Reference:* (`based on heuristic <https://www.wikidata.org/wiki/Property:P887>`_, `artificial intelligence <https://www.wikidata.org/wiki/Q11660>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP)
:func:`add_people_statements`
  | *Claim:* `Joey Ramone <https://www.wikidata.org/wiki/Q312387>`_, `member of <https://www.wikidata.org/wiki/Property:P463>`_, `Ramones <https://www.wikidata.org/wiki/Q483407>`_
  | *Reference:* (`based on heuristic <https://www.wikidata.org/wiki/Property:P887>`_, `record linkage <https://www.wikidata.org/wiki/Q1266546>`_),`(stated in <https://www.wikidata.org/wiki/Property:P248>`_, `Discogs <https://www.wikidata.org/wiki/Q504063>`_), (`Discogs artist ID <https://www.wikidata.org/wiki/Property:P1953>`_, `264375 <https://www.discogs.com/artist/264375>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP)
:func:`add_works_statements`
  | *Claim:* `Leave Home <https://www.wikidata.org/wiki/Q1346637>`_, `performer <https://www.wikidata.org/wiki/Property:P175>`_, `Ramones <https://www.wikidata.org/wiki/Q483407>`_
  | *Reference:* (`based on heuristic <https://www.wikidata.org/wiki/Property:P887>`_, `record linkage <https://www.wikidata.org/wiki/Q1266546>`_),`(stated in <https://www.wikidata.org/wiki/Property:P248>`_, `Discogs <https://www.wikidata.org/wiki/Q504063>`_), (`Discogs artist ID <https://www.wikidata.org/wiki/Property:P1953>`_, `264375 <https://www.discogs.com/artist/264375>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP)
:func:`delete_or_deprecate_identifiers`
  deletes or deprecates identifier statements.

.. _sandbox 2: https://www.wikidata.org/wiki/Q13406268
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import csv
import json
import logging
from datetime import date
from re import match
from typing import Iterable

import click
import pywikibot
from pywikibot.exceptions import APIError, Error, NoPageError

from soweego.commons import target_database
from soweego.commons.constants import QID_REGEX
from soweego.commons.keys import IMDB, TWITTER
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

SITE = pywikibot.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()

#######################
# BEGIN: Edit summaries
#######################
# Approved task 1: identifiers addition
# https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/Soweego_bot
IDENTIFIERS_SUMMARY = (
    '[[Wikidata:Requests_for_permissions/Bot/Soweego_bot|bot task 1]] '
    'with P887 reference, '
    'see [[Topic:V6cc1thgo09otfw5#flow-post-v7i05rpdja1b3wzk|discussion]]'
)

# Approved task 2: URLs validation, criterion 2
# https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/Soweego_bot_2
LINKS_VALIDATION_SUMMARY = (
    '[[Wikidata:Requests_for_permissions/Bot/Soweego_bot_2|bot task 2]] '
    'with extra P887 and catalog ID reference'
)

# Approved task 3: works by people
# https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/Bot/Soweego_bot_3
WORKS_SUMMARY = (
    '[[Wikidata:Requests_for_permissions/Bot/Soweego_bot_3|bot task 3]] '
    'with extra P887 reference'
)

# Biographical data validation, criterion 3
# TODO add wikilink once the bot task gets approved
BIO_VALIDATION_SUMMARY = 'bot task 4'
#####################
# END: Edit summaries
#####################

# Time stamp object for the (retrieved, TIMESTAMP) reference
TODAY = date.today()
TIMESTAMP = pywikibot.WbTime(
    site=REPO,
    year=TODAY.year,
    month=TODAY.month,
    day=TODAY.day,
    precision='day',
)

# We also support Twitter
SUPPORTED_TARGETS = target_database.supported_targets() ^ {TWITTER}


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.argument('invalid_identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
def delete_cli(catalog, entity, invalid_identifiers, sandbox):
    """Delete invalid identifiers.

    INVALID_IDENTIFIERS must be a JSON file.
    Format: { catalog_identifier: [ list of QIDs ] }
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', vocabulary.SANDBOX_2)

    delete_or_deprecate_identifiers(
        'delete', catalog, entity, json.load(invalid_identifiers), sandbox
    )


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.argument('invalid_identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
def deprecate_cli(catalog, entity, invalid_identifiers, sandbox):
    """Deprecate invalid identifiers.

    INVALID_IDENTIFIERS must be a JSON file.
    Format: { catalog_identifier: [ list of QIDs ] }
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', vocabulary.SANDBOX_2)

    delete_or_deprecate_identifiers(
        'deprecate', catalog, entity, json.load(invalid_identifiers), sandbox
    )


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.argument('identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
def identifiers_cli(catalog, entity, identifiers, sandbox):
    """Add identifiers.

    IDENTIFIERS must be a JSON file.
    Format: { QID: catalog_identifier }

    If the identifier already exists, just add a reference.

    Example:

    $ echo '{ "Q446627": "266995" }' > rhell.json

    $ python -m soweego ingester identifiers discogs musician rhell.json

    Result:

    claim (Richard Hell, Discogs artist ID, 266995)

    reference (based on heuristic, artificial intelligence), (retrieved, today)
    """
    add_identifiers(json.load(identifiers), catalog, entity, sandbox)


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('statements', type=click.File())
@click.option(
    '-c',
    '--criterion',
    type=click.Choice(('links', 'bio')),
    help='Validation criterion used to generate STATEMENTS. '
    'Same as the command passed to `python -m soweego sync`',
)
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
def people_cli(catalog, statements, criterion, sandbox):
    """Add statements to Wikidata people.

    STATEMENTS must be a CSV file.
    Format: person_QID, PID, value, person_catalog_ID

    If the claim already exists, just add a reference.

    Example:

    $ echo Q312387,P463,Q483407,264375 > joey.csv

    $ python -m soweego ingester people discogs joey.csv

    Result:

    claim (Joey Ramone, member of, Ramones)

    reference (based on heuristic, record linkage), (stated in, Discogs), (Discogs artist ID, 264375), (retrieved, today)
    """
    sandbox_item = vocabulary.SANDBOX_2
    # See https://www.wikidata.org/wiki/Wikidata:Project_chat/Archive/2021/07#URLs_statistics_for_Discogs_(Q504063)_and_MusicBrainz_(Q14005)
    heuristic = vocabulary.RECORD_LINKAGE
    catalog_qid = target_database.get_catalog_qid(catalog)
    catalog_pid = target_database.get_person_pid(catalog)

    if criterion == 'links':
        edit_summary = LINKS_VALIDATION_SUMMARY
    elif criterion == 'bio':
        edit_summary = BIO_VALIDATION_SUMMARY
    else:
        edit_summary = None

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', sandbox_item)

    stmt_reader = csv.reader(statements)
    for person, predicate, value, catalog_id in stmt_reader:
        subject = person if not sandbox else sandbox_item
        _add_or_reference(
            (subject, predicate, value),
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=catalog_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('statements', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
def works_cli(catalog, statements, sandbox):
    """Add statements to Wikidata works.

    STATEMENTS must be a CSV file.
    Format: work_QID, PID, person_QID, person_target_ID

    If the claim already exists, just add a reference.

    Example:

    $ echo Q4354548,P175,Q5969,139984 > cmon.csv

    $ python -m soweego ingester works discogs cmon.csv

    Result:

    claim (C'mon Everybody, performer, Eddie Cochran)

    reference (based on heuristic, record linkage), (stated in, Discogs), (Discogs artist ID, 139984), (retrieved, today)
    """
    sandbox_item = vocabulary.SANDBOX_2
    catalog_qid = target_database.get_catalog_qid(catalog)
    is_imdb, person_pid = _get_works_args(catalog)
    heuristic = vocabulary.RECORD_LINKAGE

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', sandbox_item)

    stmt_reader = csv.reader(statements)
    for work, predicate, person, person_id in stmt_reader:
        subject = work if not sandbox else sandbox_item
        _add_or_reference_works(
            (subject, predicate, person),
            heuristic,
            catalog_qid,
            person_pid,
            person_id,
            is_imdb=is_imdb,
            edit_summary=WORKS_SUMMARY,
        )


def add_identifiers(
    identifiers: dict, catalog: str, entity: str, sandbox: bool
) -> None:
    """Add identifier statements to existing Wikidata items.

    :param identifiers: a ``{QID: catalog_identifier}`` dictionary
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param sandbox: whether to perform edits on the Wikidata `sandbox 2`_ item
    """
    sandbox_item = vocabulary.SANDBOX_2
    catalog_pid = target_database.get_catalog_pid(catalog, entity)
    heuristic = vocabulary.ARTIFICIAL_INTELLIGENCE

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', sandbox_item)

    for qid, tid in identifiers.items():
        LOGGER.info('Processing %s match: %s -> %s', catalog, qid, tid)
        subject = qid if not sandbox else sandbox_item
        _add_or_reference(
            (
                subject,
                catalog_pid,
                tid,
            ),
            heuristic,
            edit_summary=IDENTIFIERS_SUMMARY,
        )


def add_people_statements(
    catalog: str, statements: Iterable, criterion: str, sandbox: bool
) -> None:
    """Add statements to existing Wikidata people.

    Statements typically come from validation criteria 2 or 3
    as per :func:`soweego.validator.checks.links` and
    :func:`soweego.validator.checks.bio`.

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param statements: iterable of
      (subject, predicate, value, catalog ID) tuples
    :param criterion: ``{'links', 'bio'}``. A supported validation criterion
    :param sandbox: whether to perform edits on the Wikidata `sandbox 2`_ item
    """
    if criterion == 'links':
        edit_summary = LINKS_VALIDATION_SUMMARY
    elif criterion == 'bio':
        edit_summary = BIO_VALIDATION_SUMMARY
    else:
        raise ValueError(
            f"Invalid criterion: '{criterion}'. " "Please use either 'links' or 'bio'"
        )

    sandbox_item = vocabulary.SANDBOX_2
    catalog_qid = target_database.get_catalog_qid(catalog)
    person_pid = target_database.get_person_pid(catalog)
    heuristic = vocabulary.RECORD_LINKAGE

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', sandbox_item)

    for subject, predicate, value, catalog_id in statements:
        LOGGER.info(
            'Processing (%s, %s, %s, %s) statement ...',
            subject,
            predicate,
            value,
            catalog_id,
        )
        actual_subject = subject if not sandbox else sandbox_item
        _add_or_reference(
            (actual_subject, predicate, value),
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=person_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )


def add_works_statements(statements: Iterable, catalog: str, sandbox: bool) -> None:
    """Add statements to existing Wikidata works.

    Statements typically come from
    :func:`soweego.validator.enrichment.generate_statements`.

    :param statements: iterable of
      (work QID, predicate, person QID, person target ID) tuples
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param sandbox: whether to perform edits on the Wikidata `sandbox 2`_ item
    """
    sandbox_item = vocabulary.SANDBOX_2
    catalog_qid = target_database.get_catalog_qid(catalog)
    is_imdb, person_pid = _get_works_args(catalog)
    heuristic = vocabulary.RECORD_LINKAGE

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item %s ...', sandbox_item)

    for work, predicate, person, person_id in statements:
        LOGGER.info(
            'Processing (%s, %s, %s, %s) statement',
            work,
            predicate,
            person,
            person_id,
        )
        subject = work if not sandbox else sandbox_item
        _add_or_reference_works(
            (subject, predicate, person),
            heuristic,
            catalog_qid,
            person_pid,
            person_id,
            is_imdb=is_imdb,
            edit_summary=WORKS_SUMMARY,
        )


def delete_or_deprecate_identifiers(
    action: str, catalog: str, entity: str, invalid: dict, sandbox: bool
) -> None:
    """Delete or deprecate invalid identifier statements
    from existing Wikidata items.

    Deletion candidates come from validation criterion 1
    as per :func:`soweego.validator.checks.dead_ids`.

    Deprecation candidates come from validation criteria 2 or 3
    as per :func:`soweego.validator.checks.links` and
    :func:`soweego.validator.checks.bio`.

    :param action: {'delete', 'deprecate'}
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param invalid: a ``{invalid_catalog_identifier: [list of QIDs]}`` dictionary
    :param sandbox: whether to perform edits on the Wikidata `sandbox 2`_ item
    """
    sandbox_item = vocabulary.SANDBOX_2
    catalog_pid = target_database.get_catalog_pid(catalog, entity)

    for tid, qids in invalid.items():
        for qid in qids:
            actual_qid = qid if not sandbox else sandbox_item
            LOGGER.info('Will %s %s identifier: %s -> %s', action, catalog, tid, qid)
            _delete_or_deprecate(action, actual_qid, tid, catalog, catalog_pid)


def _add_or_reference_works(
    statement: tuple,
    heuristic: str,
    catalog_qid: str,
    catalog_pid: str,
    catalog_id: str,
    is_imdb=False,
    edit_summary=None,
) -> None:
    work, predicate, person = statement
    # Parse value into an item in case of QID
    qid = match(QID_REGEX, person)
    if not qid:
        LOGGER.warning(
            "%s doesn't look like a QID, won't try to add the %s statement",
            person,
            statement,
        )
        return
    person_item = pywikibot.ItemPage(REPO, qid.group())

    subject_item, claims = _essential_checks(
        (work, predicate, person_item),
        heuristic,
        catalog_qid=catalog_qid,
        catalog_pid=catalog_pid,
        catalog_id=catalog_id,
        edit_summary=edit_summary,
    )
    if None in (subject_item, claims):
        return

    # IMDB-specific check: claims with same object item -> add reference
    if is_imdb:
        for pred in vocabulary.MOVIE_PIDS:
            if _check_for_same_value(
                claims,
                (work, pred, person_item),
                heuristic,
                catalog_qid=catalog_qid,
                catalog_pid=catalog_pid,
                catalog_id=catalog_id,
                edit_summary=edit_summary,
            ):
                return

    _handle_addition(
        claims,
        subject_item,
        predicate,
        person_item,
        heuristic,
        catalog_qid=catalog_qid,
        catalog_pid=catalog_pid,
        catalog_id=catalog_id,
        edit_summary=edit_summary,
    )


def _add_or_reference(
    statement,
    heuristic,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
    edit_summary=None,
) -> None:
    subject, predicate, value = statement
    subject_item, claims = _essential_checks(
        statement,
        heuristic,
        catalog_qid=catalog_qid,
        catalog_pid=catalog_pid,
        catalog_id=catalog_id,
        edit_summary=edit_summary,
    )

    if None in (subject_item, claims):
        return

    value = _parse_value(value)

    # If 'official website' property has the same value -> add reference
    # See https://www.wikidata.org/wiki/User_talk:Jura1#Thanks_for_your_feedback_on_User:Soweego_bot_task_2
    if _check_for_same_value(
        claims,
        (
            subject,
            vocabulary.OFFICIAL_WEBSITE,
            value,
        ),
        heuristic,
        edit_summary=edit_summary,
        catalog_qid=catalog_qid,
        catalog_pid=catalog_pid,
        catalog_id=catalog_id,
    ):
        return

    # Handle case-insensitive IDs: Facebook, Twitter
    # See https://www.wikidata.org/wiki/Topic:Unym71ais48bt6ih
    case_insensitive = predicate in (
        vocabulary.FACEBOOK_PID,
        vocabulary.TWITTER_USERNAME_PID,
    )

    _handle_addition(
        claims,
        subject_item,
        predicate,
        value,
        heuristic,
        case_insensitive=case_insensitive,
        catalog_qid=catalog_qid,
        catalog_pid=catalog_pid,
        catalog_id=catalog_id,
        edit_summary=edit_summary,
    )


def _handle_addition(
    claims,
    subject_item,
    predicate,
    value,
    heuristic,
    case_insensitive=False,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
    edit_summary=None,
):
    given_predicate_claims = claims.get(predicate)
    subject_qid = subject_item.getID()

    # No claim with the given predicate -> add statement
    if not given_predicate_claims:
        LOGGER.debug('%s has no %s claim', subject_qid, predicate)
        _add(
            subject_item,
            predicate,
            value,
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=catalog_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )
        return

    if case_insensitive:
        value = value.lower()
        existing_values = [
            claim_value.getTarget().lower()
            for claim_value in given_predicate_claims
            # Yes, it happens: a claim with no value
            if claim_value.getTarget()
        ]
    else:
        existing_values = [
            claim_value.getTarget() for claim_value in given_predicate_claims
        ]

    # No given value -> add statement
    if value not in existing_values:
        LOGGER.debug('%s has no %s claim with value %s', subject_qid, predicate, value)
        _add(
            subject_item,
            predicate,
            value,
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=catalog_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )
        return

    # Claim with the given predicate and value -> add reference
    LOGGER.debug("%s has a %s claim with value '%s'", subject_qid, predicate, value)
    if case_insensitive:
        for claim in given_predicate_claims:
            if claim.getTarget().lower() == value:
                _reference(
                    claim,
                    heuristic,
                    catalog_qid,
                    catalog_pid,
                    catalog_id,
                    edit_summary=edit_summary,
                )
                return

    for claim in given_predicate_claims:
        if claim.getTarget() == value:
            _reference(
                claim,
                heuristic,
                catalog_qid,
                catalog_pid,
                catalog_id,
                edit_summary=edit_summary,
            )


def _handle_redirect_and_dead(qid):
    item = pywikibot.ItemPage(REPO, qid)

    while item.isRedirectPage():
        item = item.getRedirectTarget()

    try:
        data = item.get()
    except NoPageError:
        LOGGER.warning("%s doesn't exist anymore", qid)
        return None, None

    return item, data


def _essential_checks(
    statement: tuple,
    heuristic: str,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
    edit_summary=None,
):
    subject, predicate, value = statement
    item, data = _handle_redirect_and_dead(subject)

    if item is None and data is None:
        return None, None

    # No data at all
    if not data:
        LOGGER.warning('%s has no data at all', subject)
        _add(
            item,
            predicate,
            value,
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=catalog_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )
        return None, None

    claims = data.get('claims')
    # No claims
    if not claims:
        LOGGER.warning('%s has no claims', subject)
        _add(
            item,
            predicate,
            value,
            heuristic,
            catalog_qid=catalog_qid,
            catalog_pid=catalog_pid,
            catalog_id=catalog_id,
            edit_summary=edit_summary,
        )
        return None, None

    return item, claims


def _check_for_same_value(
    subject_claims,
    statement,
    heuristic,
    edit_summary=None,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
):
    subject, predicate, value = statement
    given_predicate_claims = subject_claims.get(predicate)
    if given_predicate_claims:
        for claim in given_predicate_claims:
            if claim.getTarget() == value:
                LOGGER.debug(
                    "%s has a %s claim with value '%s'",
                    subject,
                    predicate,
                    value,
                )
                _reference(
                    claim,
                    heuristic,
                    catalog_qid=catalog_qid,
                    catalog_pid=catalog_pid,
                    catalog_id=catalog_id,
                    edit_summary=edit_summary,
                )
                return True
    return False


def _parse_value(value):
    # It may not be a string
    if not isinstance(value, str):
        value = str(value)
    # Build an item in case of QID
    value_is_qid = match(QID_REGEX, value)
    if value_is_qid:
        return pywikibot.ItemPage(REPO, value_is_qid.group())
    # Try to build a date
    try:
        # A date should be in the form '1984-11-16/11'
        date_str, precision = value.split('/')
        date_obj = date.fromisoformat(date_str)
        return pywikibot.WbTime(
            date_obj.year,
            date_obj.month,
            date_obj.day,
            precision=int(precision),
        )
    # Otherwise return the value as is
    except ValueError:
        return value


def _get_works_args(catalog):
    # Boolean to run IMDb-specific checks
    is_imdb = catalog == IMDB
    person_pid = target_database.get_person_pid(catalog)
    return is_imdb, person_pid


def _add(
    subject_item,
    predicate,
    value,
    heuristic,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
    edit_summary=None,
):
    claim = pywikibot.Claim(REPO, predicate)
    claim.setTarget(value)
    subject_item.addClaim(claim, summary=edit_summary)
    LOGGER.debug('Added claim: %s', claim.toJSON())
    _reference(
        claim,
        heuristic,
        catalog_qid,
        catalog_pid,
        catalog_id,
        edit_summary=edit_summary,
    )
    LOGGER.info('Added (%s, %s, %s) statement', subject_item.getID(), predicate, value)


def _reference(
    claim: pywikibot.Claim,
    heuristic: str,
    catalog_qid=None,
    catalog_pid=None,
    catalog_id=None,
    edit_summary=None,
):
    reference_node, log_buffer = [], []

    # Create `pywikibot.Claim` instances at runtime:
    # pywikibot would cry if the same instances get uploaded multiple times
    # over the same item

    # Depends on the bot task
    # (based on heuristic, `heuristic`) reference claim
    based_on_heuristic_reference = pywikibot.Claim(
        REPO, vocabulary.BASED_ON_HEURISTIC, is_reference=True
    )
    based_on_heuristic_reference.setTarget(pywikibot.ItemPage(REPO, heuristic))
    reference_node.append(based_on_heuristic_reference)
    log_buffer.append(f'({based_on_heuristic_reference.getID()}, {heuristic})')

    # Validator tasks only
    if catalog_qid is not None:
        # (stated in, CATALOG) reference claim
        stated_in_reference = pywikibot.Claim(
            REPO, vocabulary.STATED_IN, is_reference=True
        )
        stated_in_reference.setTarget(pywikibot.ItemPage(REPO, catalog_qid))
        reference_node.append(stated_in_reference)
        log_buffer.append(f'({stated_in_reference.getID()}, {catalog_qid})')

    if catalog_pid is not None and catalog_id is not None:
        # (catalog property, catalog ID) reference claim
        catalog_id_reference = pywikibot.Claim(REPO, catalog_pid, is_reference=True)
        catalog_id_reference.setTarget(catalog_id)
        reference_node.append(catalog_id_reference)
        log_buffer.append(f'({catalog_pid}, {catalog_id})')

    # All tasks
    # (retrieved, TODAY) reference claim
    retrieved_reference = pywikibot.Claim(REPO, vocabulary.RETRIEVED, is_reference=True)
    retrieved_reference.setTarget(TIMESTAMP)
    reference_node.append(retrieved_reference)
    log_buffer.append(f'({retrieved_reference.getID()}, {TODAY})')

    log_msg = ', '.join(log_buffer)

    try:
        claim.addSources(reference_node, summary=edit_summary)
        LOGGER.info('Added %s reference node', log_msg)
    except (
        APIError,
        Error,
    ) as error:
        LOGGER.warning('Could not add %s reference node: %s', log_msg, error)


def _delete_or_deprecate(action, qid, tid, catalog, catalog_pid) -> None:
    item, data = _handle_redirect_and_dead(qid)

    if item is None and data is None:
        LOGGER.error('Cannot %s %s identifier %s', action, catalog, tid)
        return

    item_claims = data.get('claims')
    # This should not happen:
    # the input item is supposed to have at least an identifier claim.
    # We never know, Wikidata is alive.
    if not item_claims:
        LOGGER.error(
            '%s has no claims. Cannot %s %s identifier %s',
            qid,
            action,
            catalog,
            tid,
        )
        return

    identifier_claims = item_claims.get(catalog_pid)
    # Same comment as the previous one
    if not identifier_claims:
        LOGGER.error(
            '%s has no %s claims. Cannot %s %s identifier %s',
            qid,
            catalog_pid,
            action,
            catalog,
            tid,
        )
        return

    for claim in identifier_claims:
        if claim.getTarget() == tid:
            if action == 'delete':
                item.removeClaims([claim], summary='Invalid identifier')
            elif action == 'deprecate':
                claim.changeRank('deprecated', summary='Deprecate arguable claim')
            LOGGER.debug('%s claim: %s', action.title() + 'd', claim.toJSON())
    LOGGER.info(
        '%s %s identifier statement from %s', action.title() + 'd', catalog, qid
    )
