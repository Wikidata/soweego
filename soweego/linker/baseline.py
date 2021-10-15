#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A rule-based linker."""

__author__ = 'Marco Fossati, Massimo Frasson'
__email__ = 'fossati@spaziodati.eu, maxfrax@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs, MaxFrax96'

import csv
import gzip
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Set, TextIO, Tuple, Union

import click
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from soweego.commons import (constants, data_gathering, keys, target_database,
                             text_utils, url_utils)
from soweego.commons.utils import count_num_lines_in_file
from soweego.importer.models.base_entity import BaseEntity
from soweego.importer.models.base_link_entity import BaseLinkEntity
from soweego.ingester import wikidata_bot
from soweego.linker.workflow import build_wikidata

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.option(
    '-r',
    '--rule',
    type=click.Choice(['perfect', 'links', 'names', 'all']),
    default='all',
    help='Activate a specific rule or all of them. Default: all.',
)
@click.option('-u', '--upload', is_flag=True, help='Upload links to Wikidata.')
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.WORK_DIR,
    help=f'Input/output directory, default: {constants.WORK_DIR}.',
)
@click.option(
    '--dates/--no-dates',
    default=True,
    help='Check if dates match, when applicable. Default: yes.',
)
def cli(catalog, entity, rule, upload, sandbox, dir_io, dates):
    """Run a rule-based linker.

    Available rules:

    'perfect' = perfect match on names

    'links' = similar match on link tokens

    'names' = similar match on name tokens

    Run all of them by default.
    """
    LOGGER.info("Running baseline '%s' rule over %s %s ...", rule, catalog, entity)

    # No need for the return value: only the output file will be consumed
    build_wikidata('classification', catalog, entity, dir_io)

    _run(catalog, entity, rule, dates, upload, sandbox, dir_io)

    LOGGER.info("Baseline '%s' rule over %s %s completed", rule, catalog, entity)


def _run(catalog, entity, rule, check_dates, upload, sandbox, dir_io):
    wd_io_path = os.path.join(
        dir_io, constants.WD_CLASSIFICATION_SET.format(catalog, entity)
    )
    base_entity = target_database.get_main_entity(catalog, entity)
    link_entity = target_database.get_link_entity(catalog, entity)

    if rule == 'links' and link_entity is None:
        LOGGER.warning(
            "No links available for %s %s. Stopping baseline here ...",
            catalog,
            entity,
        )
        return

    pid = target_database.get_catalog_pid(catalog, entity)

    with gzip.open(wd_io_path, 'rt') as wd_io:
        if rule in ('perfect', 'all'):
            wd_io.seek(0)

            LOGGER.info('Starting perfect names linker ...')

            result = _perfect_names_linker(wd_io, base_entity, pid, check_dates)

            perfect_path = os.path.join(
                dir_io, constants.BASELINE_PERFECT.format(catalog, entity)
            )
            os.makedirs(os.path.dirname(perfect_path), exist_ok=True)
            _handle_result(result, rule, catalog, perfect_path, upload, sandbox)

        if rule == 'all' and link_entity is None:
            LOGGER.warning(
                "No links available for %s %s. Won't run the 'links' rule ...",
                catalog,
                entity,
            )

        if rule in ('links', 'all') and link_entity is not None:
            wd_io.seek(0)

            LOGGER.info('Starting similar link tokens linker ...')

            result = _similar_tokens_linker(
                wd_io,
                link_entity,
                (keys.URL, keys.URL_TOKENS),
                pid,
                False,
                url_utils.tokenize,
            )

            links_path = os.path.join(
                dir_io, constants.BASELINE_LINKS.format(catalog, entity)
            )
            os.makedirs(os.path.dirname(links_path), exist_ok=True)
            _handle_result(result, rule, catalog, links_path, upload, sandbox)

        if rule in ('names', 'all'):
            wd_io.seek(0)

            LOGGER.info('Starting similar name tokens linker ...')

            result = _similar_tokens_linker(
                wd_io,
                base_entity,
                (keys.NAME, keys.NAME_TOKENS),
                pid,
                check_dates,
                text_utils.tokenize,
            )

            names_path = os.path.join(
                dir_io, constants.BASELINE_NAMES.format(catalog, entity)
            )
            os.makedirs(os.path.dirname(names_path), exist_ok=True)
            _handle_result(result, rule, catalog, names_path, upload, sandbox)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.option('-u', '--upload', is_flag=True, help='Upload links to Wikidata.')
@click.option(
    '-s',
    '--sandbox',
    is_flag=True,
    help='Perform all edits on the Wikidata sandbox item Q4115189.',
)
@click.option(
    '-d',
    '--dir-io',
    type=click.Path(file_okay=False),
    default=constants.WORK_DIR,
    help=f'Input/output directory, default: {constants.WORK_DIR}.',
)
def extract_cli(catalog, entity, upload, sandbox, dir_io):
    """Extract Wikidata links from a target catalog dump."""
    db_entity = target_database.get_link_entity(catalog, entity)

    if db_entity is None:
        LOGGER.info(
            'No links available for %s %s. Stopping extraction here',
            catalog,
            entity,
        )
        sys.exit(1)

    result_path = os.path.join(
        dir_io, constants.EXTRACTED_LINKS.format(catalog, entity)
    )
    os.makedirs(os.path.dirname(result_path), exist_ok=True)

    LOGGER.info(
        'Starting extraction of Wikidata links available in %s %s ...',
        catalog,
        entity,
    )

    _handle_result(
        _extract_existing_links(
            db_entity, target_database.get_catalog_pid(catalog, entity)
        ),
        'Wikidata links',
        catalog,
        result_path,
        upload,
        sandbox,
    )


def _extract_existing_links(db_entity, catalog_pid):
    for result in data_gathering.tokens_fulltext_search(
        db_entity,
        True,
        ('wikidata',),
        where_clause=db_entity.is_wiki == 1,
        limit=1_000_000_000,
    ):
        url = result.url
        qid = re.search(constants.QID_REGEX, url)

        if not qid:
            LOGGER.warning('Skipping URL with no Wikidata QID: %s', url)
            continue

        yield qid.group(), catalog_pid, result.catalog_id


def _handle_result(
    result: Iterable[Tuple[str, str, str]],
    origin: str,
    catalog: str,
    path_out: str,
    upload: bool,
    sandbox: bool,
):
    if upload:
        to_upload = set()  # In-memory copy of the result generator

    with open(path_out, 'w', 1) as fout:
        writer = csv.writer(fout)
        for statement in result:
            writer.writerow(statement)
            if upload:
                to_upload.add(statement)

    if upload:
        wikidata_bot.add_people_statements(catalog, to_upload, 'links', sandbox)

    LOGGER.info('%s %s dumped to %s', catalog, origin, path_out)


# Compare names via perfect match on lowercased strings
def _perfect_names_linker(
    wd_dataset: TextIO,
    target_db_entity: BaseEntity,
    catalog_pid: str,
    compare_dates: bool,
) -> Iterable[Tuple[str, str, str]]:
    bucket, bucket_names, bucket_size = [], set(), 100
    total = count_num_lines_in_file(wd_dataset)
    missing = total

    for row in tqdm(wd_dataset, total=total):
        wd_item = json.loads(row)
        bucket_names.update(wd_item[keys.NAME])
        bucket.append(wd_item)

        # Build a bucket of `bucket_size` Wikidata items
        if len(bucket) >= bucket_size or missing < bucket_size:
            missing -= len(bucket)

            # Look the names up in the target database
            for target in data_gathering.perfect_name_search_bucket(
                target_db_entity, bucket_names
            ):
                # Run a n^2 comparison and yield matches
                for wd in bucket:
                    # Wikidata items have lists of names
                    for wd_name in wd[keys.NAME]:
                        if not wd_name:
                            continue

                        if wd_name.lower() == target.name.lower():
                            if not compare_dates or _birth_death_date_match(wd, target):
                                yield wd[keys.QID], catalog_pid, target.catalog_id

            bucket.clear()
            bucket_names.clear()


# Compare pairs of token sets:
# if a set is included in the other, then it's a match.
# N.B.: sets of size 1 are always excluded
def _similar_tokens_linker(
    wd_dataset: TextIO,
    target_db_entity: Union[BaseEntity, BaseLinkEntity],
    fields: Tuple[str, str],
    catalog_pid: str,
    compare_dates: bool,
    tokenize: Callable[[str], Set[str]],
) -> Iterable[Tuple[str, str, str]]:
    wd_field, target_field = fields
    to_exclude = set()

    for row in tqdm(wd_dataset, total=count_num_lines_in_file(wd_dataset)):
        wd_item = json.loads(row)
        qid = wd_item[keys.QID]

        for wd_name in wd_item[wd_field]:
            if not wd_name:
                continue

            to_exclude.clear()

            wd_tokens = tokenize(wd_name)

            if len(wd_tokens) <= 1:
                continue

            try:
                # Check if target token sets are equal or larger
                for target in data_gathering.tokens_fulltext_search(
                    target_db_entity, True, wd_tokens
                ):
                    if not compare_dates or _birth_death_date_match(wd_item, target):
                        yield qid, catalog_pid, target.catalog_id
                        to_exclude.add(target.catalog_id)

                # Check if target token sets are smaller
                where_clause = target_db_entity.catalog_id.notin_(to_exclude)
                for target in data_gathering.tokens_fulltext_search(
                    target_db_entity,
                    False,
                    wd_tokens,
                    where_clause=where_clause,
                ):
                    target_tokens = set(getattr(target, target_field).split())

                    if len(target_tokens) > 1 and target_tokens.issubset(wd_tokens):
                        if not compare_dates or _birth_death_date_match(
                            wd_item, target
                        ):
                            yield qid, catalog_pid, target.catalog_id
            except SQLAlchemyError as error:
                LOGGER.warning(
                    "Skipping failed full-text search query due to %s. "
                    "You can enable the debug log with the CLI option "
                    "'-l soweego.linker.baseline DEBUG' for more details",
                    error.__class__.__name__,
                )
                LOGGER.debug(error)
                continue


def _compare_dates_on_shared_precision(
    common_precision: int, wd_date_parts: Iterable, target_date_parts: Iterable
) -> bool:
    # Refuse to compare when precision is less than year
    if common_precision < 9:
        return False

    for i in range(0, common_precision - 9 + 1):
        if int(wd_date_parts[i]) != int(target_date_parts[i]):
            return False

    return True


# Given a Wikidata date like `["1743-00-00T00:00:00Z", 9]`,
# a target date, and a target precision,
# check whether they match
def _dates_match(
    wd_date_and_precision: List, target_date: datetime, target_precision: int
) -> bool:
    if None in (wd_date_and_precision, target_date, target_precision):
        return False

    wd_precision = int(wd_date_and_precision[1])
    wd_date_parts = wd_date_and_precision[0].split('T')[0].split('-')
    shared_precision = min(wd_precision, target_precision)

    return _compare_dates_on_shared_precision(
        shared_precision,
        wd_date_parts,
        [target_date.year, target_date.month, target_date.day],
    )


# Given a Wikidata and target item pairs,
# check whether  born and death dates match
def _birth_death_date_match(wd_item: Dict, target_item: BaseEntity) -> bool:
    for wd_date in wd_item.get(keys.DATE_OF_BIRTH, []):
        if _dates_match(
            wd_date,
            getattr(target_item, keys.DATE_OF_BIRTH),
            getattr(target_item, keys.BIRTH_PRECISION),
        ):
            return True

    for wd_date in wd_item.get(keys.DATE_OF_DEATH, []):
        if _dates_match(
            wd_date,
            getattr(target_item, keys.DATE_OF_DEATH),
            getattr(target_item, keys.DEATH_PRECISION),
        ):
            return True

    return False
