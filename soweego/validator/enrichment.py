#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Enrichment of Wikidata based on data available in target catalogs."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import csv
import logging
import os
import sys
from itertools import product
from typing import Iterator, Tuple

import click
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from soweego.commons import constants, data_gathering, keys, target_database, utils
from soweego.commons.db_manager import DBManager
from soweego.ingester import wikidata_bot
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.option('-u', '--upload', is_flag=True, help='Upload statements to Wikidata.')
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
def works_people_cli(catalog, entity, upload, sandbox, dir_io):
    """Generate statements about works by people.

    Dump a CSV file of statements.
    Format: work_QID,PID,person_QID,person_catalog_ID

    You can pass the '-u' flag to upload the statements to Wikidata.
    """
    if upload:
        to_upload = set()

    statements = generate_statements(catalog, entity)
    if statements is None:
        sys.exit(1)

    with open(
        os.path.join(dir_io, constants.WORKS_BY_PEOPLE_STATEMENTS % (catalog, entity)),
        'w',
        1,
    ) as fout:
        writer = csv.writer(fout)
        for stmt in statements:
            writer.writerow(stmt)
            if upload:
                # Fill a set from the statements generator
                # to prevent lost connections to the SQL DB
                to_upload.add(stmt)

    if upload:
        wikidata_bot.add_works_statements(to_upload, catalog, sandbox)


def generate_statements(
    catalog: str, entity: str, bucket_size: int = 5000
) -> Iterator[Tuple]:
    """Generate statements about works by people.

    **How it works:**

    1. gather works and people identifiers of the given catalog
       from relevant Wikidata items
    2. leverage catalog relationships between works and people
    3. build Wikidata statements accordingly

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param bucket_size: (optional) how many target IDs should be looked up
      in the given catalog. For efficiency purposes
    :return: the statements ``generator``,
      yielding *(work_QID, PID, person_QID, person_catalog_ID)* ``tuple`` s
    """
    works, people = {}, {}

    # Wikidata side
    _gather_wd_data(catalog, entity, works, people)

    # Invert & simplify dictionaries for easier lookup later on
    works_inverted, people_inverted = (
        _invert_and_simplify(works),
        _invert_and_simplify(people),
    )
    del works, people  # Efficiency paranoia

    # Make buckets for target queries:
    # more queries, but more efficient ones
    total_queries, works_buckets, people_buckets = _prepare_target_queries(
        bucket_size, works_inverted, people_inverted
    )

    # Target side
    LOGGER.info(
        'Firing %d queries to the internal database, this will take a while ...',
        total_queries,
    )

    yield from _gather_target_data(
        catalog,
        entity,
        total_queries,
        works_buckets,
        works_inverted,
        people_buckets,
        people_inverted,
    )

    LOGGER.info('Queries done, statements generated')


def _gather_target_data(
    catalog,
    entity,
    total_queries,
    works_buckets,
    works_inverted,
    people_buckets,
    people_inverted,
):
    claim_pid = vocabulary.WORKS_BY_PEOPLE_MAPPING[catalog][entity]
    db_entity = target_database.get_relationship_entity(catalog, entity)
    session = DBManager().connect_to_db()

    # Leverage works-people relationships
    try:
        for works, people in tqdm(
            product(works_buckets, people_buckets), total=total_queries
        ):
            works_to_people = session.query(db_entity).filter(
                and_(
                    db_entity.from_catalog_id.in_(works),
                    db_entity.to_catalog_id.in_(people),
                )
            )

            for result in works_to_people:
                yield works_inverted[
                    result.from_catalog_id
                ], claim_pid, people_inverted[
                    result.to_catalog_id
                ], result.to_catalog_id
    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed query of works-people relationships due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.validator DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)

        session.rollback()
        return None
    finally:
        session.close()


def _prepare_target_queries(bucket_size, works_inverted, people_inverted):
    works_buckets = utils.make_buckets(
        list(works_inverted.keys()), bucket_size=bucket_size
    )
    people_buckets = utils.make_buckets(
        list(people_inverted.keys()), bucket_size=bucket_size
    )
    total_queries = len(works_buckets) * len(people_buckets)

    return total_queries, works_buckets, people_buckets


def _gather_wd_data(catalog, entity, works, people):
    # Works IDs
    data_gathering.gather_target_ids(
        target_database.get_work_type(catalog, entity),
        catalog,
        target_database.get_work_pid(catalog),
        works,
    )

    # People IDs
    data_gathering.gather_target_ids(
        entity, catalog, target_database.get_person_pid(catalog), people
    )


def _invert_and_simplify(dictionary):
    inverted = {}
    for qid, obj in dictionary.items():
        tids = obj[keys.TID]
        for tid in tids:
            qid_already_there = inverted.get(tid)
            if qid_already_there:
                LOGGER.debug(
                    'Target ID %s has multiple QIDs. Skipping %s, keeping %s',
                    tid,
                    qid,
                    qid_already_there,
                )
                continue
            inverted[tid] = qid
    return inverted
