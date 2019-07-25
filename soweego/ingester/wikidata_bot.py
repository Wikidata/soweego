#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A `Wikidata bot <https://www.wikidata.org/wiki/Wikidata:Bots>`_ that adds, deletes, or deprecates referenced statements.
Here are typical output examples.

:func:`add_identifiers`
  | *Claim:* `Joey Ramone <https://www.wikidata.org/wiki/Q312387>`_, `Discogs artist ID <https://www.wikidata.org/wiki/Property:P1953>`_, `264375 <https://www.discogs.com/artist/264375>`_
  | *Reference:* `stated in <https://www.wikidata.org/wiki/Property:P248>`_, `Discogs <https://www.wikidata.org/wiki/Q504063>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP
:func:`add_people_statements`
  | *Claim:* `Joey Ramone <https://www.wikidata.org/wiki/Q312387>`_, `member of <https://www.wikidata.org/wiki/Property:P463>`_, `Ramones <https://www.wikidata.org/wiki/Q483407>`_
  | *Reference:* `stated in <https://www.wikidata.org/wiki/Property:P248>`_, `Discogs <https://www.wikidata.org/wiki/Q504063>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP
:func:`add_works_statements`
  | *Claim:* `Leave Home <https://www.wikidata.org/wiki/Q1346637>`_, `performer <https://www.wikidata.org/wiki/Property:P175>`_, `Ramones <https://www.wikidata.org/wiki/Q483407>`_
  | *Reference:* `stated in <https://www.wikidata.org/wiki/Property:P248>`_, `Discogs <https://www.wikidata.org/wiki/Q504063>`_), (`Discogs artist ID <https://www.wikidata.org/wiki/Property:P1953>`_, `264375 <https://www.discogs.com/artist/264375>`_), (`retrieved <https://www.wikidata.org/wiki/Property:P813>`_, TIMESTAMP
:func:`delete_or_deprecate_identifiers`
  deletes or deprecates identifier statements.

"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import json
import logging
from datetime import date
from re import search
from typing import Iterable

import click
import pywikibot
from pywikibot.exceptions import NoPage

from soweego.commons import target_database
from soweego.commons.constants import QID_REGEX
from soweego.commons.keys import IMDB, TWITTER
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)

SITE = pywikibot.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()

# (stated in, CATALOG) reference object
STATED_IN_REFERENCE = pywikibot.Claim(
    REPO, vocabulary.STATED_IN, is_reference=True
)

# (retrieved, TIMESTAMP) reference object
TODAY = date.today()
TIMESTAMP = pywikibot.WbTime(
    site=REPO,
    year=TODAY.year,
    month=TODAY.month,
    day=TODAY.day,
    precision='day',
)
RETRIEVED_REFERENCE = pywikibot.Claim(
    REPO, vocabulary.RETRIEVED, is_reference=True
)
RETRIEVED_REFERENCE.setTarget(TIMESTAMP)

# We also support Twitter
SUPPORTED_TARGETS = target_database.supported_targets() ^ {TWITTER}


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.argument('invalid_identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
def delete_cli(catalog, entity, invalid_identifiers, sandbox):
    """Delete invalid identifiers.

    INVALID_IDENTIFIERS must be a JSON file.
    Format: { catalog_identifier: [ list of QIDs ] }
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item ...')

    delete_or_deprecate_identifiers(
        'delete', catalog, entity, json.load(invalid_identifiers), sandbox
    )


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.argument('invalid_identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
def deprecate_cli(catalog, entity, invalid_identifiers, sandbox):
    """Deprecate invalid identifiers.

    INVALID_IDENTIFIERS must be a JSON file.
    Format: { catalog_identifier: [ list of QIDs ] }
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item ...')

    delete_or_deprecate_identifiers(
        'deprecate', catalog, entity, json.load(invalid_identifiers), sandbox
    )


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.argument('identifiers', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
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

    reference (stated in, Discogs), (retrieved, today)
    """
    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item ...')

    add_identifiers(json.load(identifiers), catalog, entity, sandbox)


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('statements', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
def people_cli(catalog, statements, sandbox):
    """Add statements to Wikidata people.

    STATEMENTS must be a CSV file.
    Format: person_QID, PID, value

    If the claim already exists, just add a reference.

    Example:

    $ echo Q312387,P463,Q483407 > joey.csv

    $ python -m soweego ingester people discogs joey.csv

    Result:

    claim (Joey Ramone, member of, Ramones)

    reference (stated in, Discogs), (retrieved, today)
    """
    stated_in = target_database.get_catalog_qid(catalog)

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item ...')

    for statement in statements:
        person, predicate, value = statement.rstrip().split(',')
        if sandbox:
            _add_or_reference(vocabulary.SANDBOX_1, predicate, value, stated_in)
        else:
            _add_or_reference(person, predicate, value, stated_in)


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('statements', type=click.File())
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
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

    reference (stated in, Discogs), (Discogs artist ID, 139984), (retrieved, today)
    """
    catalog_qid, is_imdb, person_pid = _get_works_args(catalog)

    if sandbox:
        LOGGER.info('Running on the Wikidata sandbox item ...')

    for statement in statements:
        work, predicate, person, person_tid = statement.rstrip().split(',')
        if sandbox:
            _add_or_reference_works(
                vocabulary.SANDBOX_1,
                predicate,
                person,
                catalog_qid,
                person_pid,
                person_tid,
                is_imdb,
            )
        else:
            _add_or_reference_works(
                work,
                predicate,
                person,
                catalog_qid,
                person_pid,
                person_tid,
                is_imdb,
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
    :param sandbox: whether to perform edits on the
      `Wikidata sandbox <https://www.wikidata.org/wiki/Q4115189>`_ item
    """
    catalog_qid = target_database.get_catalog_qid(catalog)
    catalog_pid = target_database.get_catalog_pid(catalog, entity)
    for qid, tid in identifiers.items():
        LOGGER.info('Processing %s match: %s -> %s', catalog, qid, tid)
        if sandbox:
            LOGGER.debug(
                'Using Wikidata sandbox item %s as subject, instead of %s',
                vocabulary.SANDBOX_1,
                qid,
            )
            _add_or_reference(
                vocabulary.SANDBOX_1, catalog_pid, tid, catalog_qid
            )
        else:
            _add_or_reference(qid, catalog_pid, tid, catalog_qid)


def add_people_statements(
    statements: Iterable, catalog: str, sandbox: bool
) -> None:
    """Add statements to existing Wikidata people.

    Statements typically come from validation criteria 2 or 3
    as per :func:`soweego.validator.checks.links` and
    :func:`soweego.validator.checks.bio`.

    :param statements: iterable of (subject, predicate, value) triples
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param sandbox: whether to perform edits on the
      `Wikidata sandbox <https://www.wikidata.org/wiki/Q4115189>`_ item
    """
    catalog_qid = target_database.get_catalog_qid(catalog)
    for subject, predicate, value in statements:
        LOGGER.info(
            'Processing (%s, %s, %s) statement', subject, predicate, value
        )
        if sandbox:
            _add_or_reference(
                vocabulary.SANDBOX_1, predicate, value, catalog_qid
            )
        else:
            _add_or_reference(subject, predicate, value, catalog_qid)


def add_works_statements(
    statements: Iterable, catalog: str, sandbox: bool
) -> None:
    """Add statements to existing Wikidata works.

    Statements typically come from
    :func:`soweego.validator.enrichment.generate_statements`.

    :param statements: iterable of
      (work QID, predicate, person QID, person target ID) tuples
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param sandbox: whether to perform edits on the
      `Wikidata sandbox <https://www.wikidata.org/wiki/Q4115189>`_ item
    """
    catalog_qid, is_imdb, person_pid = _get_works_args(catalog)

    for work, predicate, person, person_tid in statements:
        LOGGER.info(
            'Processing (%s, %s, %s) statement', work, predicate, person
        )
        if sandbox:
            _add_or_reference_works(
                vocabulary.SANDBOX_1,
                predicate,
                person,
                catalog_qid,
                person_pid,
                person_tid,
                is_imdb=is_imdb,
            )
        else:
            _add_or_reference_works(
                work,
                predicate,
                person,
                catalog_qid,
                person_pid,
                person_tid,
                is_imdb=is_imdb,
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
    :param sandbox: whether to perform edits on the
      `Wikidata sandbox <https://www.wikidata.org/wiki/Q4115189>`_ item
    """
    catalog_pid = target_database.get_catalog_pid(catalog, entity)

    for tid, qids in invalid.items():
        for qid in qids:
            LOGGER.info(
                'Will %s %s identifier: %s -> %s', action, catalog, tid, qid
            )
            if sandbox:
                _delete_or_deprecate(
                    action, vocabulary.SANDBOX_1, tid, catalog, catalog_pid
                )
            else:
                _delete_or_deprecate(action, qid, tid, catalog, catalog_pid)


def _add_or_reference_works(
    work: str,
    predicate: str,
    person: str,
    stated_in: str,
    person_pid: str,
    person_tid: str,
    is_imdb=False,
) -> None:
    # Parse value into an item in case of QID
    qid = search(QID_REGEX, person)
    if not qid:
        LOGGER.warning(
            "%s doesn't look like a QID, won't try to add the (%s, %s, %s) statement",
            person,
            work,
            predicate,
            person,
        )
        return
    person = pywikibot.ItemPage(REPO, qid.group())

    subject_item, claims = _essential_checks(
        work,
        predicate,
        person,
        stated_in,
        person_pid=person_pid,
        person_tid=person_tid,
    )
    if None in (subject_item, claims):
        return

    # IMDB-specific check: claims with same object item -> add reference
    if is_imdb:
        for pred in vocabulary.MOVIE_PIDS:
            if _check_for_same_value(
                claims,
                work,
                pred,
                person,
                stated_in,
                person_pid=person_pid,
                person_tid=person_tid,
            ):
                return

    _handle_addition(
        claims,
        subject_item,
        predicate,
        person,
        stated_in,
        person_pid=person_pid,
        person_tid=person_tid,
    )


def _add_or_reference(
    subject: str, predicate: str, value: str, stated_in: str
) -> None:
    subject_item, claims = _essential_checks(
        subject, predicate, value, stated_in
    )

    if None in (subject_item, claims):
        return

    value = _parse_value(value)

    # If 'official website' property has the same value -> add reference
    # See https://www.wikidata.org/wiki/User_talk:Jura1#Thanks_for_your_feedback_on_User:Soweego_bot_task_2
    if _check_for_same_value(
        claims, subject, vocabulary.OFFICIAL_WEBSITE, value, stated_in
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
        stated_in,
        case_insensitive=case_insensitive,
    )


def _handle_addition(
    claims,
    subject_item,
    predicate,
    value,
    stated_in,
    case_insensitive=False,
    person_pid=None,
    person_tid=None,
):
    given_predicate_claims = claims.get(predicate)
    subject_qid = subject_item.getID()

    # No claim with the given predicate -> add statement
    if not given_predicate_claims:
        LOGGER.debug('%s has no %s claim', subject_qid, predicate)
        _add(subject_item, predicate, value, stated_in, person_pid, person_tid)
        return

    if case_insensitive:
        value = value.lower()
        existing_values = [
            claim_value.getTarget().lower()
            for claim_value in given_predicate_claims
        ]
    else:
        existing_values = [
            claim_value.getTarget() for claim_value in given_predicate_claims
        ]

    # No given value -> add statement
    if value not in existing_values:
        LOGGER.debug(
            '%s has no %s claim with value %s', subject_qid, predicate, value
        )
        _add(subject_item, predicate, value, stated_in, person_pid, person_tid)
        return

    # Claim with the given predicate and value -> add reference
    LOGGER.debug(
        "%s has a %s claim with value '%s'", subject_qid, predicate, value
    )
    if case_insensitive:
        for claim in given_predicate_claims:
            if claim.getTarget().lower() == value:
                _reference(claim, stated_in, person_pid, person_tid)
                return

    for claim in given_predicate_claims:
        if claim.getTarget() == value:
            _reference(claim, stated_in, person_pid, person_tid)


def _essential_checks(
    subject, predicate, value, stated_in, person_pid=None, person_tid=None
):
    item = pywikibot.ItemPage(REPO, subject)

    # Handle redirects
    while item.isRedirectPage():
        item = item.getRedirectTarget()

    try:
        data = item.get()
    except NoPage:
        LOGGER.warning("%s doesn't exist anymore", subject)
        return None, None

    # No data at all
    if not data:
        LOGGER.warning('%s has no data at all', subject)
        _add(item, predicate, value, stated_in, person_pid, person_tid)
        return None, None

    claims = data.get('claims')
    # No claims
    if not claims:
        LOGGER.warning('%s has no claims', subject)
        _add(item, predicate, value, stated_in, person_pid, person_tid)
        return None, None

    return item, claims


def _check_for_same_value(
    subject_claims,
    subject,
    predicate,
    value,
    stated_in,
    person_pid=None,
    person_tid=None,
):
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
                _reference(claim, stated_in, person_pid, person_tid)
                return True
    return False


def _parse_value(value):
    # It may not be a string
    if not isinstance(value, str):
        value = str(value)
    # Build an item in case of QID
    value_is_qid = search(QID_REGEX, value)
    if value_is_qid:
        return pywikibot.ItemPage(REPO, value_is_qid.group())
    # Try to build a date
    try:
        date_value = date.fromisoformat(value)
        # Precision hack: it's a year if both month and day are 1
        precision = (
            vocabulary.YEAR
            if date_value.month == 1 and date_value.day == 1
            else vocabulary.DAY
        )
        return pywikibot.WbTime(
            date_value.year,
            date_value.month,
            date_value.day,
            precision=precision,
        )
    # Otherwise return the value as is
    except ValueError:
        return value


def _get_works_args(catalog):
    # Boolean to run IMDb-specific checks
    is_imdb = catalog == IMDB
    catalog_qid = target_database.get_catalog_qid(catalog)
    person_pid = target_database.get_person_pid(catalog)
    return catalog_qid, is_imdb, person_pid


def _add(subject_item, predicate, value, stated_in, person_pid, person_tid):
    claim = pywikibot.Claim(REPO, predicate)
    claim.setTarget(value)
    subject_item.addClaim(claim)
    LOGGER.debug('Added claim: %s', claim.toJSON())
    _reference(claim, stated_in, person_pid, person_tid)
    LOGGER.info(
        'Added (%s, %s, %s) statement', subject_item.getID(), predicate, value
    )


def _reference(claim, stated_in, person_pid, person_tid):
    STATED_IN_REFERENCE.setTarget(pywikibot.ItemPage(REPO, stated_in))

    if None in (person_pid, person_tid):
        claim.addSources([STATED_IN_REFERENCE, RETRIEVED_REFERENCE])

        LOGGER.info(
            'Added (%s, %s), (%s, %s) reference node',
            STATED_IN_REFERENCE.getID(),
            stated_in,
            RETRIEVED_REFERENCE.getID(),
            TODAY,
        )
    else:
        tid_reference = pywikibot.Claim(REPO, person_pid, is_reference=True)
        tid_reference.setTarget(person_tid)

        claim.addSources(
            [STATED_IN_REFERENCE, tid_reference, RETRIEVED_REFERENCE]
        )

        LOGGER.info(
            'Added (%s, %s), (%s, %s), (%s, %s) reference node',
            STATED_IN_REFERENCE.getID(),
            stated_in,
            person_pid,
            person_tid,
            RETRIEVED_REFERENCE.getID(),
            TODAY,
        )


def _delete_or_deprecate(action, qid, tid, catalog, catalog_pid) -> None:
    item = pywikibot.ItemPage(REPO, qid)
    item_data = item.get()
    item_claims = item_data.get('claims')
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
                claim.changeRank(
                    'deprecated', summary='Deprecate arguable claim'
                )
            LOGGER.debug('%s claim: %s', action.title() + 'd', claim.toJSON())
    LOGGER.info(
        '%s %s identifier statement from %s', action.title() + 'd', catalog, qid
    )
