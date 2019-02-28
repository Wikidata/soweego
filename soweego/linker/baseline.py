#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

import dateutil
from dateutil.relativedelta import relativedelta

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import gzip
import json
import logging
from os import path
from typing import Iterable, Tuple

import click
from sqlalchemy.exc import ProgrammingError

from soweego.commons import (data_gathering, target_database, text_utils,
                             url_utils)
from soweego.importer.models.base_entity import BaseEntity
from soweego.ingestor import wikidata_bot
from soweego.wikidata.api_requests import get_data_for_linker

LOGGER = logging.getLogger(__name__)
WD_IO_FILENAME = 'wikidata_%s_dataset.jsonl.gz'


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('-s', '--strategy', type=click.Choice(['perfect', 'links', 'names', 'all']), default='perfect')
@click.option('--check-dates/--no-check-dates', default=True,
              help='When available, checks if the dates match too. Default: yes.')
@click.option('--upload/--no-upload', default=False, help='Upload check results to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default='/app/shared',
              help="default: '/app/shared'")
def cli(target, target_type, strategy, check_dates, upload, sandbox, output_dir):
    """Rule-based matching strategies.

    NOTICE: not all the entity types are available for all the targets

    Available strategies are:
    'perfect' = perfect strings;
    'links' = similar links;
    'names' = similar names.

    Run all of them by default.
    """
    LOGGER.info(
        'Starting baseline %s strategy for %s %s ...', strategy, target, target_type)
    # Wikidata
    wd_io_path = path.join(output_dir, WD_IO_FILENAME % target)
    if not path.exists(wd_io_path):
        qids = data_gathering.gather_qids(
            target_type, target, target_database.get_pid(target))
        url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
        with gzip.open(wd_io_path, 'wt') as wd_io:
            get_data_for_linker(qids, url_pids, ext_id_pids_to_urls, wd_io, None)
            LOGGER.info("Wikidata stream stored in %s" % wd_io_path)

    target_entity = target_database.get_entity(target, target_type)
    target_link_entity = target_database.get_link_entity(target, target_type)
    target_pid = target_database.get_pid(target)

    result = None

    with gzip.open(wd_io_path, "rt") as wd_io:
        if strategy == 'perfect' or strategy == 'all':
            LOGGER.info("Starting perfect name match")
            result = perfect_name_match(
                wd_io, target_entity, target_pid, check_dates)
            _write_or_upload_result(
                strategy, target, result, output_dir, "baseline_perfect_name.csv", upload, sandbox)
        if strategy == 'links' or strategy == 'all':
            LOGGER.info("Starting similar links match")
            result = similar_link_tokens_match(
                wd_io, target_link_entity, target_pid)
            _write_or_upload_result(
                strategy, target, result, output_dir, "baseline_similar_links.csv", upload, sandbox)
        if strategy == 'names' or strategy == 'all':
            LOGGER.info("Starting similar names match")
            result = similar_name_tokens_match(
                wd_io, target_entity, target_pid, check_dates)
            _write_or_upload_result(
                strategy, target, result, output_dir, "baseline_similar_names.csv", upload, sandbox)


def _write_or_upload_result(strategy, target, result: Iterable, output_dir: str, filename: str, upload: bool,
                            sandbox: bool):
    if upload:
        wikidata_bot.add_statements(
            result, target_database.get_qid(target), sandbox)
    else:
        filepath = path.join(output_dir, filename)
        with open(filepath, 'w') as filehandle:
            for res in result:
                filehandle.write('%s\n' % ";".join(res))
                filehandle.flush()
        LOGGER.info('Baseline %s strategy against %s dumped to %s',
                    strategy, target, filepath)


def perfect_name_match(source_dataset, target_entity: BaseEntity, target_pid: str, compare_dates: bool) -> Iterable[
    Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a Base Entity and a PID,
    match perfect strings and return a dataset ``[(source_id, PID, target_id), ...]``.

    This strategy applies to any object that can be
    treated as a string: names, links, etc.
    """
    for row_entity in source_dataset:
        entity = json.loads(row_entity)
        qid = entity['qid']
        for label in entity['name']:
            for res in data_gathering.perfect_name_search(target_entity, label):
                if not compare_dates or birth_death_date_match(res, entity):
                    yield (qid, target_pid, res.catalog_id)


def similar_name_tokens_match(source, target, target_pid: str, compare_dates: bool) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for row_entity in source:
        entity = json.loads(row_entity)
        qid = entity['qid']
        for label in entity['name']:
            if not label:
                continue

            to_exclude.clear()

            tokenized = text_utils.tokenize(label)
            if len(tokenized) <= 1:
                continue

            # NOTICE: sets of size 1 are always exluded
            # Looks for sets equal or bigger containing our tokens
            for res in data_gathering.tokens_fulltext_search(target, True, tokenized):
                if not compare_dates or birth_death_date_match(res, entity):
                    yield (qid, target_pid, res.catalog_id)
                    to_exclude.add(res.catalog_id)
            # Looks for sets contained in our set of tokens
            where_clause = target.catalog_id.notin_(to_exclude)
            for res in data_gathering.tokens_fulltext_search(target, False, tokenized, where_clause=where_clause):
                res_tokenized = set(res.tokens.split())
                if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                    if not compare_dates or birth_death_date_match(res, entity):
                        yield (qid, target_pid, res.catalog_id)


def similar_link_tokens_match(source, target, target_pid: str) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for row_entity in source:
        entity = json.loads(row_entity)
        qid = entity['qid']
        for url in entity['url']:
            if not url:
                continue

            to_exclude.clear()

            tokenized = url_utils.tokenize(url)
            if len(tokenized) <= 1:
                continue

            try:
                # NOTICE: sets of size 1 are always excluded
                # Looks for sets equal or bigger containing our tokens
                for res in data_gathering.tokens_fulltext_search(target, True, tokenized):
                    yield (qid, target_pid, res.catalog_id)
                    to_exclude.add(res.catalog_id)
                # Looks for sets contained in our set of tokens
                where_clause = target.catalog_id.notin_(to_exclude)
                for res in data_gathering.tokens_fulltext_search(target, False, tokenized, where_clause):
                    res_tokenized = set(res.tokens.split())
                    if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                        yield (qid, target_pid, res.catalog_id)
            except ProgrammingError as ex:
                LOGGER.error(ex)
                LOGGER.error(f'Issues searching tokens {tokenized} of {url}')


def compare_dates_on_common_precision(common_precision, date_elements1, date_elements2):
    # safety check
    if common_precision < 9:
        return False
    for i in range(0, common_precision - 9 + 1):
        if int(date_elements1[i]) != int(date_elements2[i]):
            return False
    return True


def birth_death_date_match(target_entity: BaseEntity, wikidata_entity: dict) -> bool:
    # If the are equal it means they are both none, so it's not a match.
    if wikidata_entity.get('born') is None and target_entity.born is None:
        LOGGER.debug(f'Excluded - {target_entity} AND {wikidata_entity} born dates None')
        return False
    else:
        # If only one of them is none, they are surely not a match
        if wikidata_entity.get('born') and target_entity.born:
            # foreach wikidata date, checks if it's a match
            for date_prec in wikidata_entity['born']:
                # After all this corner cases we compute the shared part of the date and we check if it's contained in both
                common_born_precision = min(target_entity.born_precision, date_prec[1])
                born_date_elements = date_prec[0].split('T')[0].split('-')
                born_target_elements = [target_entity.born.year, target_entity.born.month, target_entity.born.day]
                if not compare_dates_on_common_precision(common_born_precision, born_date_elements,
                                                         born_target_elements):
                    LOGGER.debug(
                        f'Excluded - {target_entity} and {wikidata_entity} common born precision does not match')
                    return False
        else:
            LOGGER.debug(f'Excluded - {target_entity} OR {wikidata_entity} born date is None')
            return False

    # If the are equal it means they are both none, so it's a match.
    # Due to death date can be missing
    if not (wikidata_entity.get('died') is None and target_entity.died is None):
        # If only one of them is none, they are surely not a match
        if wikidata_entity.get('died') and target_entity.died:
            # If the wikidata entity has more than one date, it's not a match
            for date_prec in wikidata_entity['died']:
                # After all this corner case we compute the shared part of the date and we check if it's contained in both
                common_death_precision = min(target_entity.died_precision, date_prec[1])
                death_date_elements = date_prec[0].split('T')[0].split('-')
                death_target_elements = [target_entity.died.year, target_entity.died.month, target_entity.died.day]
                if not compare_dates_on_common_precision(common_death_precision, death_date_elements,
                                                         death_target_elements):
                    LOGGER.debug(
                        f'Excluded - {target_entity} and {wikidata_entity} common death precision does not match')
                    return False
        else:

            LOGGER.debug(f'Excluded - {target_entity} OR {wikidata_entity} born date is None')
            return False

    return True
