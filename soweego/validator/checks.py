#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A set of checks to validate Wikidata against target catalogs."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '2.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2021, Hjfocs'

import csv
import gzip
import json
import logging
import os
import pickle
from collections import defaultdict
from typing import DefaultDict, Dict, Iterator, Tuple, Union

import click
from sqlalchemy.exc import SQLAlchemyError

from soweego.commons import constants, data_gathering, keys, target_database, text_utils
from soweego.commons.db_manager import DBManager
from soweego.ingester import wikidata_bot
from soweego.wikidata import vocabulary, api_requests
from soweego.wikidata.api_requests import get_url_blacklist

LOGGER = logging.getLogger(__name__)

# File name templates
# For all CLIs
WD_CACHE_FNAME = '{catalog}_{entity}_{criterion}_wd_cache.pkl'
IDS_TO_BE_DEPRECATED_FNAME = '{catalog}_{entity}_{criterion}_ids_to_be_deprecated.json'
SHARED_STATEMENTS_FNAME = '{catalog}_{entity}_{criterion}_shared_statements.csv'
# For `dead_ids_cli`
DEAD_IDS_FNAME = '{catalog}_{entity}_dead_ids.json'
# For `links_cli`
EXT_IDS_FNAME = '{catalog}_{entity}_external_ids_to_be_{task}.csv'
URLS_FNAME = '{catalog}_{entity}_urls_to_be_{task}.csv'
WD_URLS_FNAME = 'wikidata_urls_for_{catalog}_{entity}.txt.gz'
# For `bio_cli`
BIO_STATEMENTS_TO_BE_ADDED_FNAME = '{catalog}_{entity}_bio_statements_to_be_added.csv'
WD_STATEMENTS_FNAME = 'wikidata_statements_for_{catalog}_{entity}.csv.gz'


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-d',
    '--deprecate',
    is_flag=True,
    help='Deprecate dead identifiers: this changes their rank in Wikidata.',
)
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all deprecations on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
@click.option(
    '--dump-wikidata',
    is_flag=True,
    help='Dump identifiers gathered from Wikidata to a Python pickle.',
)
@click.option(
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def dead_ids_cli(catalog, entity, deprecate, sandbox, dump_wikidata, dir_io):
    """Check if identifiers are still alive.

    Dump a JSON file of dead ones. Format: { identifier: [ list of QIDs ] }

    Dead identifiers should get a deprecated rank in Wikidata:
    you can pass the '-d' flag to do so.
    """
    dead_ids_path = os.path.join(
        dir_io, DEAD_IDS_FNAME.format(catalog=catalog, entity=entity)
    )
    wd_cache_path = os.path.join(
        dir_io, WD_CACHE_FNAME.format(
            catalog=catalog, entity=entity, criterion='dead_ids'
        )
    )

    # Handle Wikidata cache
    if os.path.isfile(wd_cache_path):
        with open(wd_cache_path, 'rb') as cin:
            wd_cache = pickle.load(cin)
        LOGGER.info("Loaded Wikidata cache from '%s'", cin.name)
        # Discard the second return value: Wikidata cache
        dead, _ = dead_ids(catalog, entity, wd_cache=wd_cache)
    else:
        dead, wd_cache = dead_ids(catalog, entity)

    # Dump dead ids
    with open(dead_ids_path, 'w') as fout:
        # Sets are not serializable to JSON, so cast them to lists
        json.dump(
            {target_id: list(qids) for target_id, qids in dead.items()},
            fout,
            indent=2,
        )
    LOGGER.info('Dead identifiers dumped to %s', dead_ids_path)

    # Dump Wikidata cache
    if dump_wikidata:
        try:
            with open(wd_cache_path, 'wb') as cout:
                # Using the highest protocol available for the current Python
                # version should be the most efficient solution
                pickle.dump(wd_cache, cout, protocol=pickle.HIGHEST_PROTOCOL)
            LOGGER.info(
                'Identifiers gathered from Wikidata dumped to %s', wd_cache_path
            )
        except MemoryError:
            LOGGER.warning('Could not pickle the Wikidata cache: memory error')

    # Deprecate dead ids in Wikidata
    if deprecate:
        LOGGER.info('Starting deprecation of %s IDs ...', catalog)
        wikidata_bot.delete_or_deprecate_identifiers(
            'deprecate', catalog, entity, dead, sandbox
        )


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-b',
    '--blacklist',
    is_flag=True,
    help='Filter low-quality URLs through a blacklist.',
)
@click.option(
    '-u', '--upload', is_flag=True, help='Upload the output to Wikidata.'
)
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
@click.option(
    '--dump-wikidata',
    is_flag=True,
    help='Dump URLs gathered from Wikidata to a Python pickle.',
)
@click.option(
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def links_cli(
    catalog, entity, blacklist, upload, sandbox, dump_wikidata, dir_io
):
    """Validate identifiers against links.

    Dump 6 output files:

    1. catalog IDs to be deprecated. JSON format:
    {catalog_ID: [list of QIDs]}

    2. third-party IDs to be added. CSV format:
    QID,third-party_PID,third-party_ID,catalog_ID

    3. URLs to be added. CSV format:
    QID,P2888,URL,catalog_ID

    4. third-party IDs to be referenced. Same format as file #2

    5. URLs to be referenced. Same format as file #3

    6. URLs found in Wikidata but not in the target catalog.
    GZIP text format, one URL per line

    You can pass the '-u' flag to upload the output to Wikidata.

    The '-b' flag applies a URL blacklist of low-quality Web domains to file #3.
    """
    criterion = 'links'
    # Output paths
    deprecate_path = os.path.join(
        dir_io, IDS_TO_BE_DEPRECATED_FNAME.format(
            catalog=catalog, entity=entity, criterion=criterion
        )
    )
    add_ext_ids_path = os.path.join(
        dir_io, EXT_IDS_FNAME.format(
            catalog=catalog, entity=entity, task='added'
        )
    )
    add_urls_path = os.path.join(
        dir_io, URLS_FNAME.format(
            catalog=catalog, entity=entity, task='added'
        )
    )
    ref_ext_ids_path = os.path.join(
        dir_io, EXT_IDS_FNAME.format(
            catalog=catalog, entity=entity, task='referenced'
        )
    )
    ref_urls_path = os.path.join(
        dir_io, URLS_FNAME.format(
            catalog=catalog, entity=entity, task='referenced'
        )
    )
    wd_urls_path = os.path.join(
        dir_io, WD_URLS_FNAME.format(
            catalog=catalog, entity=entity
        )
    )
    wd_cache_path = os.path.join(
        dir_io, WD_CACHE_FNAME.format(
            catalog=catalog, entity=entity, criterion=criterion
        )
    )

    # Handle Wikidata cache
    if os.path.isfile(wd_cache_path):
        with open(wd_cache_path, 'rb') as cin:
            wd_cache = pickle.load(cin)
        LOGGER.info("Loaded Wikidata cache from '%s'", cin.name)
        # Discard the last return value: Wikidata cache
        deprecate, add_ext_ids, add_urls, ref_ext_ids, ref_urls, _ = links(
            catalog, entity, blacklist, wd_cache=wd_cache
        )
    else:
        # FIXME add `wd_urls` arg
        deprecate, add_ext_ids, add_urls, ref_ext_ids, ref_urls, wd_cache = links(
            catalog, entity, blacklist
        )

    # Nothing to do: the catalog doesn't contain links
    if deprecate is None:
        return

    # Dump output files
    _dump_deprecated(deprecate, deprecate_path)
    _dump_csv_output(add_ext_ids, add_ext_ids_path, 'third-party IDs to be added')
    _dump_csv_output(add_urls, add_urls_path, 'URLs to be added')
    _dump_csv_output(ref_ext_ids, ref_ext_ids_path, 'shared third-party IDs to be referenced')
    _dump_csv_output(ref_urls, ref_urls_path, 'shared URLs to be referenced')
    with gzip.open(wd_urls_path, 'wt') as gzout:
        gzout.writelines([url + '\n' for url in wd_urls])

    # Dump Wikidata cache
    if dump_wikidata:
        try:
            with open(wd_cache_path, 'wb') as cout:
                # Using the highest protocol available for the current Python
                # version should be the most efficient solution
                pickle.dump(wd_cache, cout, protocol=pickle.HIGHEST_PROTOCOL)
            LOGGER.info(
                'URLs gathered from Wikidata dumped to %s', wd_cache_path
            )
        except MemoryError:
            LOGGER.warning('Could not pickle the Wikidata cache: memory error')

    # Upload the output to Wikidata
    if upload:
        if sandbox:
            LOGGER.info(
                'Running on the Wikidata sandbox item %s ...',
                vocabulary.SANDBOX_2
            )
        LOGGER.info('Starting deprecation of %s IDs ...', catalog)
        wikidata_bot.delete_or_deprecate_identifiers(
            'deprecate', catalog, entity, deprecate, sandbox
        )
        LOGGER.info('Starting addition of external IDs to Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, add_ext_ids, criterion, sandbox
        )
        LOGGER.info('Starting addition of URLs to Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, add_urls, criterion, sandbox
        )
        LOGGER.info('Starting referencing of shared external IDs in Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, add_ext_ids, criterion, sandbox
        )
        LOGGER.info('Starting referencing of shared URLs in Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, add_urls, criterion, sandbox
        )


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-u', '--upload', is_flag=True, help='Upload the output to Wikidata.'
)
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help=f'Perform all edits on the Wikidata sandbox item {vocabulary.SANDBOX_2}.',
)
@click.option(
    '--dump-wikidata',
    is_flag=True,
    help='Dump biographical data gathered from Wikidata to a Python pickle.',
)
@click.option(
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def bio_cli(catalog, entity, upload, sandbox, dump_wikidata, dir_io):
    """Validate identifiers against biographical data.

    Look for birth/death dates, birth/death places, gender.

    Dump 3 output files:

    1. catalog IDs to be deprecated. JSON format:
    {catalog_ID: [list of QIDs]}

    2. statements to be added. CSV format:
    QID,PID,value,catalog_ID

    3. shared statements to be referenced. Same format as file #2

    You can pass the '-u' flag to upload the output to Wikidata.
    """
    criterion = 'bio'
    deprecate_path = os.path.join(
        dir_io, IDS_TO_BE_DEPRECATED_FNAME.format(
            catalog=catalog, entity=entity, criterion=criterion
        )
    )
    add_path = os.path.join(
        dir_io, BIO_STATEMENTS_TO_BE_ADDED_FNAME.format(
            catalog=catalog, entity=entity
        )
    )
    ref_path = os.path.join(
        dir_io, SHARED_STATEMENTS_FNAME.format(
            catalog=catalog, entity=entity, criterion=criterion
        )
    )
    wd_cache_path = os.path.join(
        dir_io, WD_CACHE_FNAME.format(
            catalog=catalog, entity=entity, criterion=criterion
        )
    )

    # Handle Wikidata cache
    if os.path.isfile(wd_cache_path):
        with open(wd_cache_path, 'rb') as cin:
            wd_cache = pickle.load(cin)
        LOGGER.info("Loaded Wikidata cache from '%s'", cin.name)
        # Discard the last return value: Wikidata cache
        deprecate, add, reference, _ = bio(catalog, entity, wd_cache=wd_cache)
    else:
        deprecate, add, reference, wd_cache = bio(catalog, entity)

    # Nothing to do: the catalog doesn't contain biographical data
    if deprecate is None:
        return

    # Dump output files
    _dump_deprecated(deprecate, deprecate_path)
    _dump_csv_output(add, add_path, 'statements to be added')
    _dump_csv_output(reference, ref_path, 'shared statements to be referenced')

    # Dump Wikidata cache
    if dump_wikidata:
        try:
            with open(wd_cache_path, 'wb') as cout:
                # Using the highest protocol available for the current Python
                # version should be the most efficient solution
                pickle.dump(wd_cache, cout, protocol=pickle.HIGHEST_PROTOCOL)
            LOGGER.info(
                'Biographical data  gathered from Wikidata dumped to %s', wd_cache_path
            )
        except MemoryError:
            LOGGER.warning('Could not pickle the Wikidata cache: memory error')

    # Upload the output to Wikidata:
    # deprecate, add, reference
    if upload:
        if sandbox:
            LOGGER.info(
                'Running on the Wikidata sandbox item %s ...',
                vocabulary.SANDBOX_2
            )
        LOGGER.info('Starting deprecation of %s IDs ...', catalog)
        wikidata_bot.delete_or_deprecate_identifiers(
            'deprecate', catalog, entity, deprecate, sandbox
        )
        LOGGER.info('Starting addition of extra statements to Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, add, criterion, sandbox
        )
        LOGGER.info('Starting referencing of shared statements in Wikidata ...')
        wikidata_bot.add_people_statements(
            catalog, reference, criterion, sandbox
        )


def dead_ids(
    catalog: str, entity: str, wd_cache=None
) -> Tuple[DefaultDict, Dict]:
    """Look for dead identifiers in Wikidata.
    An identifier is dead if it does not exist in the given catalog
    when this function is executed.

    Dead identifiers should be marked with a deprecated rank in Wikidata.

    **How it works:**

    1. gather identifiers of the given catalog from relevant Wikidata items
    2. look them up in the given catalog
    3. if an identifier is not in the given catalog anymore,
       it should be deprecated

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param wd_cache: (optional) a ``dict`` of identifiers gathered from Wikidata
      in a previous run
    :return: the ``dict`` pair of dead identifiers
      and identifiers gathered from Wikidata
    """
    dead = defaultdict(set)
    db_entity = target_database.get_main_entity(catalog, entity)

    # Wikidata side
    if wd_cache is None:
        wd_ids = {}
        data_gathering.gather_target_ids(
            entity,
            catalog,
            target_database.get_catalog_pid(catalog, entity),
            wd_ids,
        )
    else:
        wd_ids = wd_cache

    # Target catalog side
    session = DBManager.connect_to_db()

    try:
        for qid in wd_ids:
            for tid in wd_ids[qid][keys.TID]:
                existing = (
                    session.query(db_entity.catalog_id)
                    .filter_by(catalog_id=tid)
                    .count()
                )
                if existing == 0:
                    LOGGER.debug(
                        '%s %s identifier %s is dead', qid, catalog, tid
                    )
                    dead[tid].add(qid)
        session.commit()
    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed query of target catalog identifiers due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.validator DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)
        session.rollback()
    finally:
        session.close()

    LOGGER.info(
        'Check completed. Target: %s %s. Total dead identifiers: %d',
        catalog,
        entity,
        len(dead),
    )
    return dead, wd_ids


def links(
    catalog: str, entity: str, url_blacklist=False, wd_cache=None
) -> Union[Tuple[defaultdict, list, list, list, list, dict], Tuple[None, None, None, None, None, None]]:
    """Validate identifiers against available links.

    Also generate statements based on additional links
    found in the given catalog.
    They can be used to enrich Wikidata items.

    **How it works:**

    1. gather links from the given catalog
    2. gather links from relevant Wikidata items
    3. look for shared links between pairs of Wikidata and catalog items:

      - when the pair does not share any link,
        the catalog identifier should be marked with a deprecated rank
      - when the catalog item has more links than the Wikidata one,
        they should be added to the latter

    4. try to extract third-party identifiers from extra links

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param url_blacklist: (optional) whether to apply a blacklist
      of URL domains. Default: ``False``
    :param wd_cache: (optional) a ``dict`` of links gathered from Wikidata
      in a previous run. Default: ``None``
    :return: ``tuple`` of 6 objects

      1. ``dict`` of identifiers that should be deprecated
      2. ``list`` of third-party identifiers that should be added
      3. ``list`` of URLs that should be added
      4. ``list`` of third-party identifiers that should be referenced
      5. ``list`` of URLs that should be referenced
      6. ``dict`` of links gathered from Wikidata

    """
    # Target catalog side first:
    # enable early return in case of no target links
    target_links = data_gathering.gather_target_links(entity, catalog)
    if target_links is None:
        return None, None, None, None, None, None

    deprecate, add, reference = defaultdict(set), defaultdict(set), defaultdict(set)

    # Wikidata side
    url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
    if wd_cache is None:
        wd_links = {}
        data_gathering.gather_target_ids(
            entity,
            catalog,
            target_database.get_catalog_pid(catalog, entity),
            wd_links,
        )
        data_gathering.gather_wikidata_links(
            wd_links, url_pids, ext_id_pids_to_urls
        )
    else:
        wd_links = wd_cache

    # Validation
    _validate(keys.LINKS, wd_links, target_links, deprecate, add, reference)

    # Links to be added:
    # 1. Separate external IDs from URLs
    add_ext_ids, add_urls = data_gathering.extract_ids_from_urls(
        add, ext_id_pids_to_urls
    )
    # 2. Apply URL blacklist
    if url_blacklist:
        add_urls = _apply_url_blacklist(add_urls)

    # Links to be referenced: separate external IDs from URLs
    ref_ext_ids, ref_urls = data_gathering.extract_ids_from_urls(
        reference, ext_id_pids_to_urls
    )

    LOGGER.info(
        'Validation completed. Target: %s %s. '
        'IDs to be deprecated: %d. '
        'Third-party IDs to be added: %d. '
        'URL statements to be added: %d',
        'Third-party IDs to be referenced: %d. '
        'URL statements to be referenced: %d',
        catalog, entity,
        len(deprecate),
        len(add_ext_ids), len(add_urls),
        len(ref_ext_ids), len(ref_urls)
    )

    return deprecate, add_ext_ids, add_urls, ref_ext_ids, ref_urls, wd_links


def bio(
    catalog: str, entity: str, wd_cache=None
) -> Union[Tuple[defaultdict, Iterator, Iterator, dict], Tuple[None, None, None, None]]:
    """Validate identifiers against available biographical data.

    Look for:

    - birth and death dates
    - birth and death places
    - gender

    Also generate statements based on additional data
    found in the given catalog.
    They can be used to enrich Wikidata items.

    **How it works:**

    1. gather data from the given catalog
    2. gather data from relevant Wikidata items
    3. look for shared data between pairs of Wikidata and catalog items:

      - when the pair does not share any data,
        the catalog identifier should be marked with a deprecated rank
      - when the catalog item has more data than the Wikidata one,
        it should be added to the latter

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param wd_cache: (optional) a ``dict`` of links gathered from Wikidata
      in a previous run
    :return: a ``tuple`` of 4 objects

      1. ``dict`` of identifiers that should be deprecated
      2. ``generator`` of statements that should be added
      3. ``generator`` of shared statements that should be referenced
      4. ``dict`` of biographical data gathered from Wikidata

    """
    # Target catalog side first:
    # enable early return in case of no target data
    target_bio = data_gathering.gather_target_biodata(entity, catalog)
    if target_bio is None:
        return None, None, None, None

    deprecate, add, reference = defaultdict(set), defaultdict(set), defaultdict(set)

    # Wikidata side
    if wd_cache is None:
        wd_bio = {}
        data_gathering.gather_target_ids(
            entity,
            catalog,
            target_database.get_catalog_pid(catalog, entity),
            wd_bio,
        )
        data_gathering.gather_wikidata_biodata(wd_bio)
    else:
        wd_bio = wd_cache

    # Validation
    _validate(
        keys.BIODATA,
        wd_bio, target_bio,
        deprecate, add, reference
    )

    return deprecate, _bio_statements_generator(add), _bio_statements_generator(reference), wd_bio


def _apply_url_blacklist(url_statements):
    LOGGER.info('Applying URL blacklist ...')
    initial_input_size = len(url_statements)
    blacklist = get_url_blacklist()

    # O(nm) complexity: n = len(blacklist); m = len(url_statements)
    # Expected order of magnitude: n = 10^2; m = 10^5
    for domain in blacklist:  # 10^2
        url_statements = list(  # Slurp the filter or it won't work
            filter(
                lambda stmt: domain not in stmt[2], url_statements  # 10^5
            )
        )

    LOGGER.info(
        'Filtered %.2f%%  URLs',
        (initial_input_size - len(url_statements)) / initial_input_size * 100,
    )
    return url_statements


def _bio_statements_generator(stmts_dict):
    for (qid, tid), values in stmts_dict.items():
        for pid, value in values:
            yield qid, pid, value, tid


def _validate(criterion, wd, target_generator, deprecate, add, reference):
    LOGGER.info('Starting check against target %s ...', criterion)
    target = _consume_target_generator(target_generator)

    # Large loop size: total Wikidata class instances with identifiers,
    # e.g., 80k musicians
    for qid, data in wd.items():
        wd_data = data.get(criterion)

        # Skip when no Wikidata data
        if not wd_data:
            LOGGER.warning(
                'Skipping check: no %s available in QID %s (Wikidata side)',
                criterion,
                qid,
            )
            continue

        wd_tids = data[keys.TID]
        # 1 or tiny loop size: total target IDs per Wikidata item
        # (it should almost always be 1)
        for tid in wd_tids:
            if tid in target.keys():
                target_data = target.get(tid)

                # Skip when no target data
                if not target_data:
                    LOGGER.warning(
                        'Skipping check: no %s available in '
                        'target ID %s (target side)',
                        criterion,
                        tid,
                    )
                    continue

                shared_data, extra_data = _compute_shared_and_extra(
                    criterion, wd_data, target_data
                )

                if not shared_data:
                    LOGGER.debug(
                        'No shared %s between %s and %s. The identifier '
                        'statement should be deprecated',
                        criterion,
                        qid,
                        tid,
                    )
                    deprecate[tid].add(qid)
                else:
                    LOGGER.debug(
                        '%s and %s share these %s: %s',
                        qid,
                        tid,
                        criterion,
                        shared_data,
                    )
                    reference[(qid, tid)].update(shared_data)

                if extra_data:
                    LOGGER.debug(
                        '%s has extra %s that should be added to %s: %s',
                        tid,
                        criterion,
                        qid,
                        extra_data,
                    )
                    add[(qid, tid)].update(extra_data)
                else:
                    LOGGER.debug('%s has no extra %s', tid, criterion)

    LOGGER.info(
        'Check against target %s completed: %d IDs to be deprecated, '
        '%d Wikidata items with statements to be added, ',
        '%d Wikidata items with shared statements to be referenced',
        criterion,
        len(deprecate),
        len(add),
        len(reference),
    )


def _compute_shared_and_extra(criterion, wd_data, target_data):
    if criterion == keys.LINKS:
        shared = wd_data.intersection(target_data)
        extra = target_data.difference(wd_data)
    # Biographical validation requires more complex comparisons
    elif criterion == keys.BIODATA:
        # `wd_data` has either couples or triples: couples are dates
        wd_dates = set(filter(lambda x: len(x) == 2, wd_data))
        # No cast to `set` because `wd_data` triples hold sets themselves
        wd_other = list(filter(lambda x: len(x) == 3, wd_data))
        # In `target_data` we look for relevant date PIDs
        target_dates = set(filter(
            lambda x: x[0] in (vocabulary.DATE_OF_BIRTH, vocabulary.DATE_OF_DEATH),
            target_data
        ))
        target_other = target_data.difference(target_dates)
        shared_dates, extra_dates = _compare('dates', wd_dates, target_dates)
        shared_other, extra_other = _compare('other', wd_other, target_other)
        shared = shared_dates | shared_other
        extra = extra_dates | extra_other
    else:
        raise ValueError(
            f"Invalid validation criterion: '{criterion}'. "
            f"Please use either '{keys.LINKS}' or '{keys.BIODATA}'"
        )

    return shared, extra


def _compare(what, wd, target):
    shared, extra = set(), set()
    # Keep track of matches to avoid useless computation
    # and incorrect comparisons:
    # this happens when WD has multiple claims with
    # the same property
    wd_matches, target_matches = [], []

    for i, wd_elem in enumerate(wd):
        for j, t_elem in enumerate(target):
            # Don't compare when already matched
            if i in wd_matches or j in target_matches:
                continue

            # Don't compare different PIDs
            if wd_elem[0] != t_elem[0]:
                continue

            # Skip unexpected `None` values
            if None in (wd_elem[1], t_elem[1]):
                LOGGER.warning(
                    'Skipping unexpected %s pair with missing value(s)',
                    (wd_elem, t_elem),
                )
                continue

            inputs = (
                shared, extra, wd_matches, target_matches,
                i, wd_elem, j, t_elem
            )
            if what == 'dates':
                _compare_dates(inputs)
            elif what == 'other':
                _compare_other(inputs)
            else:
                raise ValueError(
                    f"Invalid argument: '{what}'. "
                    "Please use either 'dates' or 'other'"
                )

    return shared, extra


def _compare_other(inputs):
    shared, extra, wd_matches, target_matches, i, wd_elem, j, t_elem = inputs
    pid, qid, wd_values = wd_elem
    _, t_value = t_elem

    # Take the lowercased normalized value
    # TODO improve matching
    _, t_normalized = text_utils.normalize(t_value)
    if t_normalized in wd_values:
        shared.add((pid, qid))
        wd_matches.append(i)
        target_matches.append(j)
    else:
        t_qid = api_requests.resolve_qid(t_normalized)
        if t_qid is not None:
            extra.add((pid, t_qid))


def _compare_dates(inputs):
    shared, extra, wd_matches, target_matches, i, wd_elem, j, t_elem = inputs

    wd_timestamp, wd_precision = wd_elem[1].split('/')
    t_timestamp, t_precision = t_elem[1].split('/')
    shared_date, extra_date = _match_dates_by_precision(
        min(int(wd_precision), int(t_precision)),
        wd_elem,
        wd_timestamp,
        t_elem,
        t_timestamp,
    )
    if shared_date is not None:
        shared.add(shared_date)
        wd_matches.append(i)
        target_matches.append(j)
    elif extra_date is not None:
        extra.add(extra_date)


def _match_dates_by_precision(
    precision, wd_elem, wd_timestamp, t_elem, t_timestamp
):
    slice_indices = {
        vocabulary.YEAR: 4,
        vocabulary.MONTH: 7,
        vocabulary.DAY: 10,
    }
    index = slice_indices.get(precision)

    if index is None:
        LOGGER.info(
            "Won't try to match date pair %s: too low or too high precision",
            (wd_elem, t_elem),
        )
        return None, None

    shared, extra = None, None
    wd_simplified = wd_timestamp[:index]
    t_simplified = t_timestamp[:index]
    if wd_simplified == t_simplified:
        LOGGER.debug(
            'Date pair %s matches on %s',
            (wd_timestamp, t_timestamp),
            (wd_simplified, t_simplified),
        )
        # WD data has the priority
        shared = wd_elem
    else:
        LOGGER.debug('Target has an extra date: %s', t_timestamp)
        extra = t_elem
    return shared, extra


def _dump_deprecated(data, outpath):
    if data:
        with open(outpath, 'w') as deprecated:
            json.dump(
                {target_id: list(qids) for target_id, qids in data.items()},
                deprecated,
                indent=2,
            )
        LOGGER.info('IDs to be deprecated dumped to %s', outpath)
    else:
        LOGGER.info("No IDs to be deprecated, won't dump to file")


def _dump_csv_output(data, outpath, log_msg_subject):
    if data:
        with open(outpath, 'w') as ids_out:
            writer = csv.writer(ids_out)
            writer.writerows(data)
        LOGGER.info('%s dumped to %s', log_msg_subject, outpath)
    else:
        LOGGER.info("No %s, won't dump to file", log_msg_subject)


def _consume_target_generator(target_generator):
    target = defaultdict(set)
    for identifier, *data in target_generator:
        if len(data) == 1:  # Links
            target[identifier].add(data.pop())
        else:  # Biographical data
            target[identifier].add(tuple(data))
    return target
