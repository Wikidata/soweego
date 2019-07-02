#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A rule-based linker."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import csv
import gzip
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Iterable, Tuple

import click
from sqlalchemy.exc import ProgrammingError
from tqdm import tqdm

from soweego.commons import (
    constants,
    data_gathering,
    keys,
    target_database,
    text_utils,
    url_utils,
)
from soweego.commons.utils import count_num_lines_in_file
from soweego.importer.models.base_entity import BaseEntity
from soweego.ingestor import wikidata_bot
from soweego.wikidata.api_requests import get_data_for_linker

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument(
    'target', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'target_type', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-s',
    '--strategy',
    type=click.Choice(['perfect', 'links', 'names', 'all']),
    default='all',
)
@click.option(
    '--check-dates/--no-check-dates',
    default=True,
    help='When available, checks if the dates match too. Default: yes.',
)
@click.option(
    '--upload/--no-upload',
    default=False,
    help='Upload check results to Wikidata. Default: no.',
)
@click.option(
    '--sandbox/--no-sandbox',
    default=False,
    help='Upload to the Wikidata sandbox item Q4115189. Default: no.',
)
@click.option(
    '-o',
    '--output-dir',
    type=click.Path(file_okay=False),
    default=constants.SHARED_FOLDER,
    help="default: '%s" % constants.SHARED_FOLDER,
)
def cli(
    target, target_type, strategy, check_dates, upload, sandbox, output_dir
):
    """Run a rule-based linker.

    NOTICE: not all the entity types are available for all the targets

    Available strategies are:
    'perfect' = perfect strings;
    'links' = similar links;
    'names' = similar names.

    Run all of them by default.
    """
    LOGGER.info(
        'Starting baseline %s strategy for %s %s ...',
        strategy,
        target,
        target_type,
    )

    wd_io_path = os.path.join(
        dir_io, constants.WD_CLASSIFICATION_SET % (catalog, entity)
    )

    if not os.path.isfile(wd_io_path):
        qids = data_gathering.gather_qids(
            target_type,
            target,
            target_database.get_catalog_pid(target, target_type),
        )
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(
                target,
                target_type,
                qids,
                url_pids,
                ext_id_pids_to_urls,
                None,
                wd_io,
            )
            LOGGER.info("Wikidata stream stored in %s" % wd_io_path)

    target_entity = target_database.get_main_entity(target, target_type)
    target_link_entity = target_database.get_link_entity(target, target_type)
    target_pid = target_database.get_catalog_pid(target, target_type)

    with gzip.open(wd_io_path, "rt") as wd_io:
        if strategy == 'perfect' or strategy == 'all':
            wd_io.seek(0)  # go to beginning of file
            LOGGER.info("Starting perfect name match")
            result = _perfect_name_match(
                wd_io, target_entity, target_pid, check_dates
            )
            perfect_path = os.path.join(
                output_dir,
                constants.BASELINE_PERFECT.format(target, target_type)
            )
            _handle_result(result, strategy, target, perfect_path, upload,
                           sandbox)

        if strategy == 'links' or strategy == 'all':
            wd_io.seek(0)  # go to beginning of file
            LOGGER.info("Starting similar links match")
            result = _similar_link_tokens_match(
                wd_io, target_link_entity, target_pid
            )
            links_path = os.path.join(
                output_dir,
                constants.BASELINE_LINKS.format(target, target_type)
            )
            _handle_result(result, strategy, target, links_path, upload,
                           sandbox)

        if strategy == 'names' or strategy == 'all':
            wd_io.seek(0)
            LOGGER.info("Starting similar names match")
            result = _similar_name_tokens_match(
                wd_io, target_entity, target_pid, check_dates
            )
            names_path = os.path.join(
                output_dir,
                constants.BASELINE_NAMES.format(target, target_type)
            )
            _handle_result(result, strategy, target, names_path, upload,
                           sandbox)


@click.command()
@click.argument(
    'catalog', type=click.Choice(target_database.supported_targets())
)
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.option(
    '-u',
    '--upload',
    is_flag=True,
    help='Upload links to Wikidata.',
)
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
    default=constants.SHARED_FOLDER,
    help=f'Input/output directory, default: {constants.SHARED_FOLDER}.',
)
def extract_cli(catalog, entity, upload, sandbox, dir_io):
    """Extract Wikidata links from a target catalog dump."""
    db_entity = target_database.get_link_entity(catalog, entity)

    if db_entity is None:
        LOGGER.info(
            'No links available for %s %s. Stopping extraction here',
            catalog, entity
        )
        sys.exit(1)

    result_path = os.path.join(
        dir_io,
        constants.EXTRACTED_LINKS.format(catalog, entity)
    )

    LOGGER.info(
        'Starting extraction of Wikidata links available in %s %s ...',
        catalog, entity
    )

    _handle_result(_extract_existing_links(
        db_entity,
        target_database.get_catalog_pid(catalog, entity)
    ), 'Wikidata links', catalog, result_path, upload, sandbox)


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


def _handle_result(result: Iterable[Tuple[str, str, str]], origin: str,
                   catalog: str, path_out: str, upload: bool, sandbox: bool):
    if upload:
        to_upload = set()  # In-memory copy of the result generator

    with open(path_out, 'w', 1) as fout:
        writer = csv.writer(fout)
        for statement in result:
            writer.writerow(statement)
            if upload:
                to_upload.add(statement)

    if upload:
        wikidata_bot.add_people_statements(to_upload, catalog, sandbox)

    LOGGER.info('%s %s dumped to %s', catalog, origin, path_out)


def _perfect_name_match(
    source_dataset,
    target_entity: BaseEntity,
    target_pid: str,
    compare_dates: bool,
) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a Base Entity and a PID,
    match perfect strings and return a dataset ``[(source_id, PID, target_id), ...]``.

    This strategy applies to any object that can be
    treated as a string: names, links, etc.
    """
    bucket_size = 100
    bucket_names = set()
    bucket = []
    total = count_num_lines_in_file(source_dataset)
    missing = total
    for row_entity in tqdm(source_dataset, total=total):
        entity = json.loads(row_entity)
        bucket_names.update(entity[keys.NAME])
        bucket.append(entity)
        # After building a bucket of bucket_size wikidata entries,
        # tries to search them and does a n^2 comparison to try to match
        if len(bucket) >= bucket_size or missing < bucket_size:
            missing -= len(bucket)
            for res in data_gathering.perfect_name_search_bucket(
                target_entity, bucket_names
            ):
                for en in bucket:
                    # wikidata entities have a list of names
                    for name in en[keys.NAME]:
                        if name.lower() == res.name.lower():
                            if not compare_dates or _birth_death_date_match(
                                res, en
                            ):
                                yield (en[keys.QID], target_pid, res.catalog_id)
            bucket.clear()
            bucket_names.clear()


def _similar_name_tokens_match(
    source, target, target_pid: str, compare_dates: bool
) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for row_entity in tqdm(source, total=count_num_lines_in_file(source)):
        entity = json.loads(row_entity)
        qid = entity[keys.QID]
        for label in entity[keys.NAME]:
            if not label:
                continue

            to_exclude.clear()

            tokenized = text_utils.tokenize(label)
            if len(tokenized) <= 1:
                continue

            # NOTICE: sets of size 1 are always exluded
            # Looks for sets equal or bigger containing our tokens
            for res in data_gathering.tokens_fulltext_search(
                target, True, tokenized
            ):
                if not compare_dates or _birth_death_date_match(res, entity):
                    yield (qid, target_pid, res.catalog_id)
                    to_exclude.add(res.catalog_id)
            # Looks for sets contained in our set of tokens
            where_clause = target.catalog_id.notin_(to_exclude)
            for res in data_gathering.tokens_fulltext_search(
                target, False, tokenized, where_clause=where_clause
            ):
                res_tokenized = set(res.name_tokens.split())
                if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                    if not compare_dates or _birth_death_date_match(res, entity):
                        yield (qid, target_pid, res.catalog_id)


def _similar_link_tokens_match(
    source, target, target_pid: str
) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """

    if target is None:
        return

    to_exclude = set()

    for row_entity in tqdm(source, total=count_num_lines_in_file(source)):
        entity = json.loads(row_entity)
        qid = entity[keys.QID]
        for url in entity[keys.URL]:
            if not url:
                continue

            to_exclude.clear()

            tokenized = url_utils.tokenize(url)
            if len(tokenized) <= 1:
                continue

            try:
                # NOTICE: sets of size 1 are always excluded
                # Looks for sets equal or bigger containing our tokens
                for res in data_gathering.tokens_fulltext_search(
                    target, True, tokenized
                ):
                    yield (qid, target_pid, res.catalog_id)
                    to_exclude.add(res.catalog_id)
                # Looks for sets contained in our set of tokens
                where_clause = target.catalog_id.notin_(to_exclude)
                for res in data_gathering.tokens_fulltext_search(
                    target, False, tokenized, where_clause
                ):
                    res_tokenized = set(res.url_tokens.split())
                    if len(res_tokenized) > 1 and res_tokenized.issubset(
                        tokenized
                    ):
                        yield (qid, target_pid, res.catalog_id)
            except ProgrammingError as ex:
                LOGGER.error(ex)
                LOGGER.error(f'Issues searching tokens {tokenized} of {url}')


def _compare_dates_on_common_precision(
    common_precision: int, date_elements1: Iterable, date_elements2: Iterable
) -> bool:
    # safety check
    if common_precision < 9:
        return False
    for i in range(0, common_precision - 9 + 1):
        if int(date_elements1[i]) != int(date_elements2[i]):
            return False
    return True


def _date_equals(
    born: datetime, born_precision: int, date_prec: Iterable
) -> bool:
    """Given a target date, its precision and a Wikidata date like ["1743-00-00T00:00:00Z", 9],
    tells if they're equal
    """
    if born is None or born_precision is None or not date_prec:
        return False
    prec = int(date_prec[1])
    date_elements = date_prec[0].split('T')[0].split('-')
    common_precision = min(born_precision, prec)
    return _compare_dates_on_common_precision(
        common_precision, date_elements, [born.year, born.month, born.day]
    )


def _birth_death_date_match(
    target_entity: BaseEntity, wikidata_entity: dict
) -> bool:
    """Given a wikidata json and a BaseEntity, checks born/death dates and tells if they match"""
    for date_prec in wikidata_entity.get('born', []):
        if _date_equals(
            target_entity.born, target_entity.born_precision, date_prec
        ):
            return True

    for date_prec in wikidata_entity.get('died', []):
        if _date_equals(
            target_entity.died, target_entity.died_precision, date_prec
        ):
            return True

    return False
