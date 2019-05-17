#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Performs some naive linking techniques"""
import gzip
import json
import logging
import re
from datetime import datetime
from os import path
from typing import Iterable, Tuple

import click
from sqlalchemy.exc import ProgrammingError
from tqdm import tqdm

from soweego.commons import (constants, data_gathering, target_database,
                             text_utils, url_utils)
from soweego.commons.utils import count_num_lines_in_file
from soweego.importer.models.base_entity import BaseEntity
from soweego.ingestor import wikidata_bot
from soweego.wikidata.api_requests import get_data_for_linker

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

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
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="default: '%s" % constants.SHARED_FOLDER)
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
            get_data_for_linker(target, qids, url_pids, ext_id_pids_to_urls, wd_io, None)
            LOGGER.info("Wikidata stream stored in %s" % wd_io_path)

    target_entity = target_database.get_entity(target, target_type)
    target_link_entity = target_database.get_link_entity(target, target_type)
    target_pid = target_database.get_pid(target)

    result = None

    with gzip.open(wd_io_path, "rt") as wd_io:
        if strategy == 'perfect' or strategy == 'all':
            wd_io.seek(0)  # go to beginning of file
            LOGGER.info("Starting perfect name match")
            result = perfect_name_match(
                wd_io, target_entity, target_pid, check_dates)
            _write_or_upload_result(
                strategy, target, result, output_dir, "baseline_perfect_name.csv", upload, sandbox)

        if strategy == 'links' or strategy == 'all':
            wd_io.seek(0)  # go to beginning of file
            LOGGER.info("Starting similar links match")
            result = similar_link_tokens_match(
                wd_io, target_link_entity, target_pid)
            _write_or_upload_result(
                strategy, target, result, output_dir, "baseline_similar_links.csv", upload, sandbox)

        if strategy == 'names' or strategy == 'all':
            wd_io.seek(0)
            LOGGER.info("Starting similar names match")
            result = similar_name_tokens_match(
                wd_io, target_entity, target_pid, check_dates)
            _write_or_upload_result(
                strategy, target, target_type, result, output_dir, "baseline_similar_names.csv", upload, sandbox)


@click.command()
@click.argument('target', type=click.Choice(target_database.available_targets()))
@click.argument('target_type', type=click.Choice(target_database.available_types()))
@click.option('--upload/--no-upload', default=False, help='Upload check results to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-o', '--output-dir', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="default: '%s" % constants.SHARED_FOLDER)
def extract_available_matches_in_target(target, target_type, upload, sandbox, output_dir):
    """"""
    target_link_entity = target_database.get_link_entity(target, target_type)
    target_pid = target_database.get_pid(target)

    def result_generator(target_link_entity, target_pid):
        if target_link_entity:
            for r in data_gathering.tokens_fulltext_search(target_link_entity, True, ('wikidata',),
                                                           where_clause=target_link_entity.is_wiki == 1,
                                                           limit=1_000_000_000):
                qid = re.search(r"(Q\d+)$", r.url).groups()[0]
                if qid:
                    yield (qid, target_pid, r.catalog_id)

    _write_or_upload_result('extract', target, target_type, result_generator(target_link_entity, target_pid),
                            output_dir,
                            'match_extractor.csv', upload, sandbox)


def _write_or_upload_result(strategy, target, target_type, result: Iterable, output_dir: str, filename: str,
                            upload: bool,
                            sandbox: bool):
    if upload:
        wikidata_bot.add_statements(
            result, target_database.get_qid(target), sandbox)
    else:
        filename = f'{target}_{target_type}_{filename}'
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
    bucket_size = 100
    bucket_names = set()
    bucket = []
    total = count_num_lines_in_file(source_dataset)
    missing = total
    for row_entity in tqdm(source_dataset, total=total):
        entity = json.loads(row_entity)
        bucket_names.update(entity[constants.NAME])
        bucket.append(entity)
        # After building a bucket of bucket_size wikidata entries,
        # tries to search them and does a n^2 comparison to try to match
        if len(bucket) >= bucket_size or missing < bucket_size:
            missing -= len(bucket)
            for res in data_gathering.perfect_name_search_bucket(target_entity, bucket_names):
                for en in bucket:
                    # wikidata entities have a list of names
                    for name in en[constants.NAME]:
                        if name.lower() == res.name.lower():
                            if not compare_dates or birth_death_date_match(res, en):
                                yield (en[constants.QID], target_pid, res.catalog_id)
            bucket.clear()
            bucket_names.clear()


def similar_name_tokens_match(source, target, target_pid: str, compare_dates: bool) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for row_entity in tqdm(source, total=count_num_lines_in_file(source)):
        entity = json.loads(row_entity)
        qid = entity[constants.QID]
        for label in entity[constants.NAME]:
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
                res_tokenized = set(res.name_tokens.split())
                if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                    if not compare_dates or birth_death_date_match(res, entity):
                        yield (qid, target_pid, res.catalog_id)


def similar_link_tokens_match(source, target, target_pid: str) -> Iterable[Tuple[str, str, str]]:
    """Given a dictionary ``{string: identifier}``, a BaseEntity and a tokenization function and a PID,
    match similar tokens and return a dataset ``[(source_id, PID, target_id), ...]``.

    Similar tokens match means that if a set of tokens is contained in another one, it's a match.
    """
    to_exclude = set()

    for row_entity in tqdm(source, total=count_num_lines_in_file(source)):
        entity = json.loads(row_entity)
        qid = entity[constants.QID]
        for url in entity[constants.URL]:
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
                    res_tokenized = set(res.url_tokens.split())
                    if len(res_tokenized) > 1 and res_tokenized.issubset(tokenized):
                        yield (qid, target_pid, res.catalog_id)
            except ProgrammingError as ex:
                LOGGER.error(ex)
                LOGGER.error(f'Issues searching tokens {tokenized} of {url}')


def compare_dates_on_common_precision(common_precision: int, date_elements1: Iterable,
                                      date_elements2: Iterable) -> bool:
    # safety check
    if common_precision < 9:
        return False
    for i in range(0, common_precision - 9 + 1):
        if int(date_elements1[i]) != int(date_elements2[i]):
            return False
    return True


def date_equals(born: datetime, born_precision: int, date_prec: Iterable) -> bool:
    """Given a target date, its precision and a Wikidata date like ["1743-00-00T00:00:00Z", 9],
    tells if they're equal
    """
    if born is None or born_precision is None or not date_prec:
        return False
    prec = int(date_prec[1])
    date_elements = date_prec[0].split('T')[0].split('-')
    common_precision = min(born_precision, prec)
    return compare_dates_on_common_precision(common_precision, date_elements, [born.year, born.month, born.day])


def birth_death_date_match(target_entity: BaseEntity, wikidata_entity: dict) -> bool:
    """Given a wikidata json and a BaseEntity, checks born/death dates and tells if they match"""
    for date_prec in wikidata_entity.get('born', []):
        if date_equals(target_entity.born, target_entity.born_precision, date_prec):
            return True

    for date_prec in wikidata_entity.get('died', []):
        if date_equals(target_entity.died, target_entity.died_precision, date_prec):
            return True

    return False
